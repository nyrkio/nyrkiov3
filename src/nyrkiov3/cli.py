"""``nyrkio-sync`` — one-shot backfill for a repo's workflow runs.

    nyrkio-sync --repo unodb-dev/unodb --workflow benchmarks.yml \
                --parser google-benchmark/text --step "Run benchmarks"

Configuration goes through ``nyrkiov3.config.load_sync_config``: CLI
flags, ``NYRKIO_*`` env vars, or YAML config files (see
``config.py``). The GitHub token can come from ``--github-token``,
``$NYRKIO_APP_GITHUB_PAT``, ``$CLAUDE_GITHUB_PAT``, or
``$GITHUB_TOKEN`` — whichever is set first wins.

Writes straight to the configured storage directory (embedded
secantusdb). Intended for manual backfills; the webhook path is the
long-term answer."""
from __future__ import annotations

import logging
import os
import sys

from .config import load_sync_config


def _resolve_parser(spec: str):
    """``framework/format`` -> benchzoo parser module."""
    try:
        from benchzoo.parsers import find_parser
    except ImportError as e:
        raise SystemExit(f"benchzoo not installed: {e}")
    if "/" not in spec:
        raise SystemExit(f"--parser expects framework/format, got {spec!r}")
    framework, fmt = spec.split("/", 1)
    parser = find_parser(framework, fmt)
    if parser is None:
        raise SystemExit(f"unknown parser {spec!r}")
    return parser


def main(argv: list[str] | None = None) -> int:
    cfg = load_sync_config(argv)

    logging.basicConfig(
        level=logging.DEBUG if cfg["verbose"] >= 2 else
              logging.INFO if cfg["verbose"] else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if "/" not in cfg["repo"]:
        raise SystemExit("--repo expects owner/repo")
    owner, repo = cfg["repo"].split("/", 1)

    if not cfg["github_token"]:
        raise SystemExit(
            "no GitHub token: pass --github-token, set NYRKIO_APP_GITHUB_PAT, "
            "CLAUDE_GITHUB_PAT, or GITHUB_TOKEN"
        )

    parser = _resolve_parser(cfg["parser"])

    # Local store — skip the HTTP hop for a CLI invocation. Imported
    # lazily so `nyrkio-sync -h` doesn't pay the jsonee import cost.
    # Embedded secantusdb persisted to the storage dir (the on-disk
    # stand-in for FerretDB/MongoDB); set NYRKIO_MONGO_URI to point at a
    # real server instead.
    from jsonee import open_store
    from .github_ingest import GitHubClient, ingest_workflow_history

    storage_path = cfg["storage_path"]
    os.makedirs(storage_path, exist_ok=True)
    store = open_store(storage_path=storage_path)
    client = GitHubClient(cfg["github_token"])

    try:
        summary = ingest_workflow_history(
            client=client, store=store,
            owner=owner, repo=repo,
            workflow_filename=cfg["workflow"],
            parser=parser,
            step_name=cfg["step"],
            branch=cfg["branch"],
            max_pages=cfg["max_pages"],
        )
    finally:
        store.stop()  # close client, stop embedded server
    print(f"{summary['runs_seen']} workflow runs walked, "
          f"{summary['benchmarks_inserted']} benchmarks inserted into "
          f"{storage_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
