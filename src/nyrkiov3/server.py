"""Production entry point. `python -m nyrkiov3.server` (or the
installed `nyrkio-serve` console script) builds the app with
persistent storage and serves it via uvicorn.

Env vars (all optional unless noted):

- ``NYRKIO_SNAPSHOT_PATH``     where the store dumps JSON (default
                               /var/lib/nyrkio/store.json).
- ``NYRKIO_STATIC_DIR``        path to AuroraBorealis/static/ — mounted
                               at ``/`` so the SPA serves alongside
                               the API. Omit to run API-only.
- ``NYRKIO_BIND``              host:port (default ``127.0.0.1:8123``).
                               Stay on localhost when nginx or another
                               reverse proxy fronts the app.
- ``NYRKIO_GITHUB_CLIENT_ID``  OAuth app credentials. Without these,
- ``NYRKIO_GITHUB_CLIENT_SECRET`` /login returns 503 and the landing
                               page shows the public-repo path only.
- ``NYRKIO_SESSION_SECRET``    32+ random bytes for cookie HMAC.
- ``NYRKIO_BASE_URL``          public origin+path the service is
                               served from (e.g.
                               ``https://staging.nyrkio.com/v3``).
                               Used for OAuth ``redirect_uri`` and
                               all post-redirect ``Location`` headers.
- ``CLAUDE_GITHUB_PAT`` or
  ``NYRKIO_APP_GITHUB_PAT``    app-level token used by the public
                               repo exploration path and by the
                               webhook handler.
"""
from __future__ import annotations

import logging
import os
import sys

from .app import build_app, DEFAULT_SNAPSHOT_PATH


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    snapshot_path = os.environ.get("NYRKIO_SNAPSHOT_PATH", DEFAULT_SNAPSHOT_PATH)
    app = build_app(snapshot_path=snapshot_path)
    # App-level token (for /public/connect and the webhook path).
    app.github_token = (os.environ.get("NYRKIO_APP_GITHUB_PAT")
                        or os.environ.get("CLAUDE_GITHUB_PAT") or None)
    static_dir = os.environ.get("NYRKIO_STATIC_DIR")
    if static_dir and os.path.isdir(static_dir):
        app.static("/", static_dir)
        print(f"static: serving {static_dir} at /")
    # Bind address. Keep on localhost when a same-host reverse proxy
    # fronts the app. When nginx runs inside Docker on Linux and the
    # app runs on the host, you'll typically need to bind on the
    # Docker bridge address (or 0.0.0.0) instead — that's a
    # per-deploy call, not a code one.
    raw_bind = os.environ.get("NYRKIO_BIND")
    bind = raw_bind or "127.0.0.1:8123"
    if raw_bind is None:
        print("NYRKIO_BIND not set; defaulting to 127.0.0.1:8123")
    else:
        print(f"NYRKIO_BIND={raw_bind!r}")
    host, _, port = bind.rpartition(":")
    host = host or "127.0.0.1"
    try:
        import uvicorn
    except ImportError:
        print("uvicorn not installed. `pip install uvicorn` (or uv sync) and rerun.",
              file=sys.stderr)
        return 1
    n = app.store.collection("test_runs").count()
    print(f"store has {n} runs (snapshot at {snapshot_path})")
    print(f"listening on http://{host}:{port}  (base_url={app.auth_config['base_url']})")
    uvicorn.run(app, host=host, port=int(port), log_level="info")
    return 0


if __name__ == "__main__":
    sys.exit(main())
