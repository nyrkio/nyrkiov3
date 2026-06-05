"""Configuration loaders for ``nyrkio-serve`` and ``nyrkio-sync``.

The shared plumbing — config-file discovery, the CLI/env/YAML
precedence rule, the standard JsonEE service options — lives in
:mod:`jsonee.config`. This module just registers the
nyrkiov3-specific options on top and runs the small post-parse
normalisation (the ``CLAUDE_GITHUB_PAT`` / ``GITHUB_TOKEN`` fallback
chain) that JsonEE doesn't know about.

Config files are auto-discovered under the JsonEE app name
``nyrkiov3``:

  ``/etc/nyrkiov3/config.yml`` + ``secrets.yml``
  ``~/.nyrkiov3/config.yml``   + ``secrets.yml``
  ``./nyrkiov3.yml``           + ``nyrkiov3-secrets.yml``

Env vars stay on the historic ``NYRKIO_*`` prefix (one ``v3`` letter
shorter than the app name)."""
from __future__ import annotations

import os
from typing import Any

def _build_serve_parser(p):
    # App-specific: static asset roots. Both are paths.
    p.add("--static-dir", env_var=p._jsonee_env_prefix + "STATIC_DIR", default=None,
          help="path to nyrkiov3/static/, mounted at /; "
               "omit to run API-only")
    p._jsonee_path_options.append("static_dir")
    p.add("--aurora-dir", env_var=p._jsonee_env_prefix  + "AURORA_DIR", default=None,
          help="path to AuroraBorealis/static/, mounted at "
               "/js/lib/aurora/")
    p._jsonee_path_options.append("aurora_dir")

    # App-specific: GitHub OAuth + session secrets.
    p.add("--github-client-id", env_var=p._jsonee_env_prefix  + "GITHUB_CLIENT_ID",
          default="", help="GitHub OAuth app client ID")
    p.add("--github-client-secret", env_var=p._jsonee_env_prefix  + "GITHUB_CLIENT_SECRET",
          default="",
          help="GitHub OAuth app client secret (SECRET — secrets.yml or env)")
    p.add("--session-secret", env_var=p._jsonee_env_prefix  + "SESSION_SECRET", default="",
          help="32+ random bytes for cookie HMAC (SECRET)")
    p.add("--app-github-pat", env_var=p._jsonee_env_prefix  + "APP_GITHUB_PAT", default=None,
          help="GitHub PAT used by /connect and the webhook handler "
               "(SECRET); CLAUDE_GITHUB_PAT is checked as a fallback")
    return p


def _build_sync_parser(p):
    # Per-invocation, CLI-only options.
    p.add("--repo", required=True,
          help="owner/repo, e.g. unodb-dev/unodb")
    p.add("--workflow", required=True,
          help="workflow filename, e.g. benchmarks.yml")
    p.add("--parser", required=True,
          help="benchzoo parser spec, e.g. google-benchmark/text")
    p.add("--step", default=None,
          help="optional ##[group] step name to slice the log to")
    p.add("--branch", default=None)
    p.add("--max-pages", type=int, default=5)

    # Snapshot location: reachable via config / env so a fleet of
    # sync jobs can share storage. Reuses STORAGE_PATH because that
    # IS the snapshot's parent directory.
    p.add("--snapshot-path", env_var=ENV_PREFIX + "STORAGE_PATH",
          default=os.path.join(os.path.expanduser(DEFAULT_STORAGE_PATH),
                               "secantus.snapshot"),
          help="where the InMemoryStore snapshots to disk")
    p._jsonee_path_options.append("snapshot_path")
    p.add("--snapshot-interval", env_var=ENV_PREFIX + "SNAPSHOT_INTERVAL",
          type=float, default=DEFAULT_SNAPSHOT_INTERVAL_S,
          help="seconds between snapshots (default %(default)s)")

    p.add("--github-token", env_var=ENV_PREFIX + "APP_GITHUB_PAT", default=None,
          help="GitHub PAT; CLAUDE_GITHUB_PAT and GITHUB_TOKEN are "
               "checked as fallbacks")
    p.add("-v", "--verbose", action="count", default=0)
    return p


def load_serve_config(argv: list[str] | None = None) -> dict[str, Any]:
    """Parse ``nyrkio-serve`` config; return a normalized dict."""
    cfg = jsonee_config.parse(_build_serve_parser(), argv)
    # CLAUDE_GITHUB_PAT is a host-wide convention (cf. nyrkio-sync's
    # fallback chain). Treat it as the final fallback so a developer
    # logged into a Claude-managed shell doesn't have to set anything
    # extra to ingest public repos.
    if not cfg.get("app_github_pat"):
        cfg["app_github_pat"] = os.environ.get("CLAUDE_GITHUB_PAT") or None
    return cfg


def load_sync_config(argv: list[str] | None = None) -> dict[str, Any]:
    """Parse ``nyrkio-sync`` config; return a normalized dict."""
    cfg = jsonee_config.parse(_build_sync_parser(), argv)
    if not cfg.get("github_token"):
        cfg["github_token"] = (
            os.environ.get("CLAUDE_GITHUB_PAT")
            or os.environ.get("GITHUB_TOKEN")
            or None
        )
    return cfg
