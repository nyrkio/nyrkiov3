"""Nyrkiö v3 — benchmark ingest, change detection, and query service.

Built on JsonEE (HTTP + store adapter) and benchzoo (parsers). The
service is minimal by design: a few routes on top of a FerretDB-shaped
store, plus a GitHub ingester that walks workflow runs, parses logs
via benchzoo, and writes to the store."""
from .app import build_app, SCHEMAS, DEFAULT_SNAPSHOT_PATH, DEFAULT_SNAPSHOT_INTERVAL_S
