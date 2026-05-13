"""Production entry point. ``python -m nyrkiov3.server`` (or the
installed ``nyrkio-serve`` console script) builds the app with
persistent storage and serves it via uvicorn.

All configuration goes through ``nyrkiov3.config.load_serve_config``,
which reads from (precedence high → low):

  1. command-line flags (``--bind``, ``--static-dir`` …)
  2. environment variables (``NYRKIO_BIND``, ``NYRKIO_STATIC_DIR`` …)
  3. YAML config files in ``/etc/nyrkiov3/``, ``~/.nyrkiov3/``, and
     the current directory (see ``config.py`` for the full list)
  4. compiled-in defaults

Run ``nyrkio-serve -h`` for the full option matrix; every option
displays its CLI flag, env var, and config-file key together."""
from __future__ import annotations

import logging
import os
import sys

from .app import build_app
from .config import load_serve_config


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    cfg = load_serve_config(argv)

    app = build_app(
        storage_path=cfg["storage_path"],
        mongo_uri=cfg["mongo_uri"],
        mongo_db=cfg["mongo_db"],
        auth_config={
            "client_id": cfg["github_client_id"],
            "client_secret": cfg["github_client_secret"],
            "session_secret": cfg["session_secret"],
            "base_url": cfg["base_url"],
        },
    )
    app.mount_client()  # jsonee.js at /js/lib/jsonee.js
    app.github_token = cfg["app_github_pat"]

    if cfg["static_dir"] and os.path.isdir(cfg["static_dir"]):
        app.static("/", cfg["static_dir"])
        print(f"static: serving {cfg['static_dir']} at /")
    if cfg["aurora_dir"] and os.path.isdir(cfg["aurora_dir"]):
        app.static("/js/lib/aurora", cfg["aurora_dir"])
        print(f"static: serving {cfg['aurora_dir']} at /js/lib/aurora/")

    host, _, port = cfg["bind"].rpartition(":")
    host = host or "127.0.0.1"
    try:
        import uvicorn
    except ImportError:
        print("uvicorn not installed. `pip install uvicorn` (or uv sync) and rerun.",
              file=sys.stderr)
        return 1

    n = app.store.collection("test_runs").count()
    store_desc = cfg["mongo_uri"] if cfg["mongo_uri"] else cfg["storage_path"]
    print(f"store has {n} runs ({store_desc})")
    print(f"listening on http://{host}:{port}  "
          f"(base_url={app.auth_config['base_url']})")
    uvicorn.run(app, host=host, port=int(port), log_level="info")
    return 0


if __name__ == "__main__":
    sys.exit(main())
