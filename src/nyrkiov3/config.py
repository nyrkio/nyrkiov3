"""Configuration loaders for ``nyrkio-serve`` and ``nyrkio-sync``.

The shared plumbing — config-file discovery, the CLI/env/YAML
precedence rule, the standard JsonEE service options — lives in
:mod:`jsonee.config`. This module registers the nyrkiov3-specific
options on top, fixes the per-app defaults that JsonEE leaves generic,
and runs the small post-parse normalisation (drop the ``-c`` housekeeping
key, expand ``~`` in path options, apply the GitHub-token fallbacks).

Env vars use the historic ``NYRKIO_`` prefix (one ``v3`` letter shorter
than the app name). Config files are auto-discovered under app name
``nyrkiov3`` — see :func:`jsonee.config.default_config_files`.

``nyrkio-serve`` itself is built directly from the JsonEE app lifecycle
(see :mod:`nyrkiov3.server`), which calls :func:`_build_serve_parser`.
:func:`load_serve_config` is the standalone equivalent used by tests and
any scripted caller that wants a plain config dict without a live app.
"""
from __future__ import annotations

import os
from typing import Any

from jsonee import config as jsonee_config


APP_NAME = "nyrkiov3"
ENV_PREFIX = "NYRKIO_"  # not NYRKIOV3_ — preserves the v2-inherited convention
DEFAULT_MONGO_DB = "nyrkio"
DEFAULT_BIND = "127.0.0.1:8123"
DEFAULT_BASE_URL = "https://nyrkio.com"
DEFAULT_STORAGE_PATH = "~/data/secantus"  # mirrors app.DEFAULT_STORAGE_PATH
DEFAULT_SNAPSHOT_INTERVAL_S = 60.0

# Exposed so tests can monkeypatch a clean discovery list, and so callers
# can introspect where config is read from.
DEFAULT_CONFIG_FILES = jsonee_config.default_config_files(APP_NAME)


def _base_parser(prog: str, description: str = ""):
    """A JsonEE parser carrying nyrkiov3's app-level defaults.

    :func:`jsonee.config.create_parser` registers the common service
    options with generic defaults; we override the few that differ for
    this app (``mongo-db``, ``base-url``) and point discovery at *our*
    (monkeypatch-able) :data:`DEFAULT_CONFIG_FILES`. CLI flags, env vars
    and config files all still win over these defaults."""
    p = jsonee_config.create_parser(
        APP_NAME, prog=prog, description=description, env_prefix=ENV_PREFIX)
    p.set_defaults(
        mongo_db=DEFAULT_MONGO_DB,
        base_url=DEFAULT_BASE_URL,
        bind=DEFAULT_BIND,
        storage_path=DEFAULT_STORAGE_PATH,
    )
    p._default_config_files = list(DEFAULT_CONFIG_FILES)
    return p


def _build_serve_parser(p):
    """Add the nyrkio-serve-specific options to a JsonEE parser ``p``.

    Called both by :func:`load_serve_config` here and by
    :func:`nyrkiov3.server.main` on the live app's parser."""
    pref = p._jsonee_env_prefix
    # App-specific: static asset roots. Both are paths.
    p.add("--static-dir", env_var=pref + "STATIC_DIR", default=None,
          help="path to nyrkiov3/static/, mounted at /; omit to run API-only")
    p._jsonee_path_options.append("static_dir")
    p.add("--aurora-dir", env_var=pref + "AURORA_DIR", default=None,
          help="path to AuroraBorealis/static/, mounted at /js/lib/aurora/")
    p._jsonee_path_options.append("aurora_dir")
    # App-specific: GitHub OAuth + session secrets.
    p.add("--github-client-id", env_var=pref + "GITHUB_CLIENT_ID",
          default="", help="GitHub OAuth app client ID")
    p.add("--github-client-secret", env_var=pref + "GITHUB_CLIENT_SECRET",
          default="", help="GitHub OAuth app client secret (SECRET)")
    p.add("--session-secret", env_var=pref + "SESSION_SECRET", default="",
          help="32+ random bytes for cookie HMAC (SECRET)")
    p.add("--app-github-pat", env_var=pref + "APP_GITHUB_PAT", default=None,
          help="GitHub PAT used by /connect and the webhook handler "
               "(SECRET); CLAUDE_GITHUB_PAT is checked as a fallback")
    return p


def _build_sync_parser(p):
    """Add the nyrkio-sync-specific options to a JsonEE parser ``p``."""
    pref = p._jsonee_env_prefix
    p.add("--repo", required=True, help="owner/repo, e.g. unodb-dev/unodb")
    p.add("--workflow", required=True,
          help="workflow filename, e.g. benchmarks.yml")
    p.add("--parser", required=True,
          help="benchzoo parser spec, e.g. google-benchmark/text")
    p.add("--step", default=None,
          help="optional ##[group] step name to slice the log to")
    p.add("--branch", default=None)
    p.add("--max-pages", type=int, default=5)
    # Snapshot location for nyrkio-sync's InMemoryStore. Reuses the
    # STORAGE_PATH env var (the snapshot's parent is the storage dir).
    p.add("--snapshot-path", env_var=pref + "STORAGE_PATH",
          default=os.path.join(os.path.expanduser(DEFAULT_STORAGE_PATH),
                               "secantus.snapshot"),
          help="where the InMemoryStore snapshots to disk")
    p._jsonee_path_options.append("snapshot_path")
    p.add("--snapshot-interval", env_var=pref + "SNAPSHOT_INTERVAL",
          type=float, default=DEFAULT_SNAPSHOT_INTERVAL_S,
          help="seconds between snapshots (default %(default)s)")
    p.add("--github-token", env_var=pref + "APP_GITHUB_PAT", default=None,
          help="GitHub PAT; CLAUDE_GITHUB_PAT and GITHUB_TOKEN are "
               "checked as fallbacks")
    p.add("-v", "--verbose", action="count", default=0)
    return p


def _parse(p, argv: list[str] | None) -> dict[str, Any]:
    """Parse argv and normalise: drop the ``-c`` housekeeping key and
    expand ``~`` in any option tagged path-typed by ``create_parser`` /
    the ``_build_*`` helpers."""
    ns = p.parse_args(argv)
    cfg = vars(ns).copy()
    cfg.pop("config_file", None)
    for key in getattr(p, "_jsonee_path_options", ()):
        if cfg.get(key):
            cfg[key] = os.path.expanduser(cfg[key])
    return cfg


def load_serve_config(argv: list[str] | None = None) -> dict[str, Any]:
    """Parse ``nyrkio-serve`` config; return a normalized dict."""
    p = _build_serve_parser(
        _base_parser("nyrkio-serve", "Nyrkiö v3 ingest + read service."))
    cfg = _parse(p, argv)
    # CLAUDE_GITHUB_PAT is a host-wide convention: final fallback so a
    # developer in a Claude-managed shell can ingest public repos with
    # no extra setup.
    if not cfg.get("app_github_pat"):
        cfg["app_github_pat"] = os.environ.get("CLAUDE_GITHUB_PAT") or None
    return cfg


def load_sync_config(argv: list[str] | None = None) -> dict[str, Any]:
    """Parse ``nyrkio-sync`` config; return a normalized dict."""
    p = _build_sync_parser(
        _base_parser("nyrkio-sync",
                     "One-shot backfill for a repo's workflow runs."))
    cfg = _parse(p, argv)
    # --github-token > NYRKIO_APP_GITHUB_PAT (both via the option) >
    # CLAUDE_GITHUB_PAT > GITHUB_TOKEN.
    if not cfg.get("github_token"):
        cfg["github_token"] = (
            os.environ.get("CLAUDE_GITHUB_PAT")
            or os.environ.get("GITHUB_TOKEN")
            or None
        )
    return cfg
