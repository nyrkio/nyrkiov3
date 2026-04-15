"""``nyrkio-sync`` — one-shot backfill for a repo's workflow runs.

    nyrkio-sync --repo unodb-dev/unodb --workflow benchmarks.yml \
                --parser google-benchmark/text --step "Run benchmarks"

Reads the token from ``$CLAUDE_GITHUB_PAT`` (or ``$GITHUB_TOKEN``) and
writes straight to the configured store's snapshot path. Intended for
manual backfills; the webhook path is the long-term answer."""
from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys

from .app import DEFAULT_SNAPSHOT_PATH, DEFAULT_SNAPSHOT_INTERVAL_S
from .github_ingest import GitHubClient, ingest_workflow_history


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
    ap = argparse.ArgumentParser(prog="nyrkio-sync",
                                 description=__doc__.splitlines()[0])
    ap.add_argument("--repo", required=True,
                    help="owner/repo, e.g. unodb-dev/unodb")
    ap.add_argument("--workflow", required=True,
                    help="workflow filename, e.g. benchmarks.yml")
    ap.add_argument("--parser", required=True,
                    help="benchzoo parser spec, e.g. google-benchmark/text")
    ap.add_argument("--step", default=None,
                    help="optional ##[group] step name to slice the log to")
    ap.add_argument("--branch", default=None)
    ap.add_argument("--max-pages", type=int, default=5)
    ap.add_argument("--snapshot-path", default=DEFAULT_SNAPSHOT_PATH)
    ap.add_argument("--snapshot-interval", type=float,
                    default=DEFAULT_SNAPSHOT_INTERVAL_S)
    ap.add_argument("-v", "--verbose", action="count", default=0)
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose >= 2 else
              logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if "/" not in args.repo:
        raise SystemExit("--repo expects owner/repo")
    owner, repo = args.repo.split("/", 1)

    token = os.environ.get("CLAUDE_GITHUB_PAT") or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise SystemExit(
            "no GitHub token in $CLAUDE_GITHUB_PAT or $GITHUB_TOKEN")

    parser = _resolve_parser(args.parser)

    # Use the store directly, not HTTP ingest, so we skip the HTTP hop
    # for a local CLI invocation.
    from jsonee import InMemoryStore
    os.makedirs(os.path.dirname(args.snapshot_path), exist_ok=True)
    store = InMemoryStore(snapshot_path=args.snapshot_path,
                          snapshot_interval_s=args.snapshot_interval)
    client = GitHubClient(token)

    summary = ingest_workflow_history(
        client=client, store=store,
        owner=owner, repo=repo,
        workflow_filename=args.workflow,
        parser=parser,
        step_name=args.step,
        branch=args.branch,
        max_pages=args.max_pages,
    )
    # Final flush (stop also triggers snapshot).
    store.stop()
    print(f"{summary['runs_seen']} workflow runs walked, "
          f"{summary['benchmarks_inserted']} benchmarks inserted into "
          f"{args.snapshot_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
