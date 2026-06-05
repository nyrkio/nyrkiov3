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

from jsonee.app import JsonEE

from .config import _build_serve_parser
from .app import SCHEMAS, build_app

def main(argv: list[str] | None = None) -> int:
    # TODO
    #app.github_token = cfg["app_github_pat"]

    # env_prefix stays NYRKIO_ (not NYRKIOV3_): preserves the v2-inherited
    # convention and matches the NYRKIO_MONGO_URI / NYRKIO_MONGO_DB names
    # that jsonee.store reads directly.
    jsonapp = JsonEE(app_name="nyrkiov3", env_prefix="NYRKIO_", description="Nyrkiö continuous benchmarking analytics", schema_registry=SCHEMAS)
    p = jsonapp.parser()
    _build_serve_parser(p)

    jsonapp.parse_args(argv)
    jsonapp.open_store()
    logging.debug(jsonapp.cfg)

    jsonapp.auth_config={
        "client_id": jsonapp.cfg["github_client_id"],
        "client_secret": jsonapp.cfg["github_client_secret"],
        "session_secret": jsonapp.cfg["session_secret"],
        "base_url": jsonapp.cfg["base_url"],
    }

    if jsonapp.cfg["static_dir"] and os.path.isdir(jsonapp.cfg["static_dir"]):
        jsonapp.static("/", jsonapp.cfg["static_dir"])
        print(f"static: serving {jsonapp.cfg['static_dir']} at /")
    if jsonapp.cfg["aurora_dir"] and os.path.isdir(jsonapp.cfg["aurora_dir"]):
        jsonapp.static("/js/lib/aurora", jsonapp.cfg["aurora_dir"])
        print(f"static: serving {jsonapp.cfg['aurora_dir']} at /js/lib/aurora/")

    build_app(jsonapp)
    jsonapp.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
