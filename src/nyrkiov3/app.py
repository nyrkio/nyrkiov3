"""Nyrkiö v3 app: ingest + read endpoints, benchzoo-shaped payload,
FerretDB-compatible store interface. Schemas in SCHEMAS are authoritative."""
import datetime
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from purejson import Document, Collection
from extjson import ObjectId, dumps, utcnow, parse_date, to_utc, UTC
from jsonee import JsonEE, Request, Response, HTTPError, InMemoryStore


LOG = logging.getLogger("nyrkiov3.app")


# Snapshot config defaults. Overridable via build_app(store=…) when a
# caller wants a pre-built store (tests, pure in-memory demos).
DEFAULT_SNAPSHOT_DIR = "/home/claude/data"
DEFAULT_SNAPSHOT_PATH = os.path.join(DEFAULT_SNAPSHOT_DIR, "nyrkio-store.json")
DEFAULT_SNAPSHOT_INTERVAL_S = 60.0


SCHEMAS = {
    "Repo": {
        "$id": "Repo",
        "type": "object",
        "properties": {
            "_id": {"type": "objectid"},
            "platform": {"type": "string", "enum": ["gh", "gl"]},
            "namespace": {"type": "string"},
            "repo": {"type": "string"},
            "absolute_name": {"type": "string"},
            "visibility": {"type": "string", "enum": ["public", "private"]},
            "installed_at": {"type": "date"},
        },
        "required": ["platform", "namespace", "repo", "absolute_name"],
    },
    "Metric": {
        "$id": "Metric",
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "unit": {"type": "string"},
            "value": {"type": "number"},
        },
        "required": ["name", "value"],
    },
    "TestRun": {
        "$id": "TestRun",
        "type": "object",
        "properties": {
            "_id": {"type": "objectid"},
            "repo_id": {"type": "objectid"},
            "absolute_name": {"type": "string"},
            "branch": {"type": "string"},
            "git_commit": {"type": "string"},
            "timestamp": {"type": "date"},
            "attributes": {"type": "object"},
            "metrics": {"type": "array", "items": "Metric"},
            "extra_info": {"type": "object"},
            "passed": {"type": "boolean"},
            "source": {"type": "object"},
        },
        "required": ["absolute_name", "timestamp", "attributes", "metrics", "passed"],
    },
    "IngestPayload": {
        "$id": "IngestPayload",
        "type": "object",
        "properties": {
            "runs": {"type": "array", "items": "IngestRun"},
        },
        "required": ["runs"],
    },
    # Ingest format: looks like benchzoo's output — repo fields optional on input,
    # the endpoint fills them from path params.
    "IngestRun": {
        "$id": "IngestRun",
        "type": "object",
        "properties": {
            "branch": {"type": "string"},
            "git_commit": {"type": "string"},
            "timestamp": {"type": ["date", "string", "integer"]},
            "attributes": {"type": "object"},
            "metrics": {"type": "array", "items": "Metric"},
            "extra_info": {"type": "object"},
            "passed": {"type": "boolean"},
        },
        "required": ["attributes", "metrics"],
    },
}


def build_app(store=None, recent_cp_days=14, snapshot_path=None,
              snapshot_interval_s=DEFAULT_SNAPSHOT_INTERVAL_S):
    """Build the v0 nyrkio app.

    - ``store``: pre-built store to use (tests, in-memory demos). Takes
      precedence over ``snapshot_path``.
    - ``snapshot_path``: if set (and no ``store`` passed), the default
      in-memory store restores from this file on startup and dumps to
      it every ``snapshot_interval_s`` seconds. Pass
      ``DEFAULT_SNAPSHOT_PATH`` for the canonical location.
    """
    if store is None:
        if snapshot_path:
            os.makedirs(os.path.dirname(snapshot_path), exist_ok=True)
            store = InMemoryStore(
                snapshot_path=snapshot_path,
                snapshot_interval_s=snapshot_interval_s,
            )
        else:
            store = InMemoryStore()
    app = JsonEE(schema_registry=SCHEMAS)
    app.store = store  # attach for tests / handlers
    # Background executor for work that shouldn't block HTTP handlers —
    # webhooks, periodic rebuilds, change-point detection. On Python
    # 3.14 free-threaded these threads run concurrently with each other
    # and with the request path; on a GIL build they still decouple the
    # response from the work.
    app.background = ThreadPoolExecutor(
        max_workers=4, thread_name_prefix="nyrkio-bg")
    # Tunables that downstream tools (flyover, Slack summary) read.
    # `recent_cp_days` is the "recent window": any commit within that many
    # days of now() is interesting enough to visit in the daily scan even
    # if it carries no change points; older commits only surface when they
    # do have CPs.
    app.config = {"recent_cp_days": recent_cp_days}

    @app.route("GET", "/api/v3/config")
    def get_config(request: Request):
        return Document(app.config)

    @app.route("POST", "/api/v3/webhooks/github")
    def github_webhook(request: Request):
        """GitHub ``workflow_run`` webhook entry point.

        Validates the HMAC signature synchronously (so invalid callers
        get 401 immediately), then submits the ingest work to the
        background executor and returns 202. GitHub retries on
        non-2xx, so we must not 500 on parse failure — the background
        task logs and moves on.
        """
        headers = request.get("headers") or {}
        event = headers.get("x-github-event", "")
        if event != "workflow_run":
            return Document(ignored=True, reason=f"event={event}")
        secret = getattr(app, "github_webhook_secret", None)
        if secret:
            import hmac as _hmac
            import hashlib as _hashlib
            sig = headers.get("x-hub-signature-256", "")
            raw = request.get("raw_body") or b""
            expected = "sha256=" + _hmac.new(
                secret.encode("utf-8"), raw, _hashlib.sha256).hexdigest()
            if not _hmac.compare_digest(sig, expected):
                raise HTTPError(401, "invalid webhook signature")
        token = getattr(app, "github_token", None)
        parsers = getattr(app, "github_parsers", None)
        default_parser = getattr(app, "github_default_parser", None)
        if token is None or (parsers is None and default_parser is None):
            raise HTTPError(503, "github ingest not configured")
        payload = request["body"]

        def _do_ingest():
            try:
                from .github_ingest import GitHubClient, handle_workflow_run_event
                client = GitHubClient(token)
                inserted = handle_workflow_run_event(
                    client=client, store=store, payload=payload,
                    parsers=parsers, default_parser=default_parser,
                    step_name=getattr(app, "github_step_name", None),
                )
                LOG.info("webhook ingest: run %s → +%d benchmarks",
                         (payload.get("workflow_run") or {}).get("id"), inserted)
            except Exception:
                LOG.exception("webhook ingest failed for run %s",
                              (payload.get("workflow_run") or {}).get("id"))

        app.background.submit(_do_ingest)
        return Response(
            body=Document(
                accepted=True,
                run_id=(payload.get("workflow_run") or {}).get("id"),
            ),
            status=202,
        )

    repos = store.collection("repos")
    runs = store.collection("test_runs")

    def _ensure_repo(platform, namespace, repo):
        absolute = f"{platform}/{namespace}/{repo}"
        existing = repos.find_one({"absolute_name": absolute})
        if existing is not None:
            return existing
        doc = Document(
            platform=platform,
            namespace=namespace,
            repo=repo,
            absolute_name=absolute,
            installed_at=utcnow(),
        )
        repos.insert_one(doc)
        return repos.find_one({"absolute_name": absolute})

    @app.route("POST", "/api/v3/ingest/{platform}/{namespace}/{repo}",
               body_schema="IngestPayload")
    def ingest(request: Request):
        params = request["path_params"]
        platform, namespace, repo = params["platform"], params["namespace"], params["repo"]
        if platform not in ("gh", "gl"):
            raise HTTPError(400, f"unsupported platform {platform!r}")

        repo_doc = _ensure_repo(platform, namespace, repo)
        absolute = repo_doc["absolute_name"]

        payload_runs = request["body"]["runs"]
        inserted = []
        for raw in payload_runs:
            ts = raw.get("timestamp")
            if isinstance(ts, (int, float)):
                ts = datetime.datetime.fromtimestamp(ts, tz=UTC)
            elif isinstance(ts, str):
                ts = parse_date(ts)  # strict: rejects naive ISO strings
            elif isinstance(ts, datetime.datetime):
                ts = to_utc(ts)
            elif ts is None:
                ts = utcnow()

            doc = Document(
                repo_id=repo_doc["_id"],
                absolute_name=absolute,
                branch=raw.get("branch", "main"),
                git_commit=raw.get("git_commit", ""),
                timestamp=ts,
                attributes=raw["attributes"],
                metrics=raw["metrics"],
                extra_info=raw.get("extra_info", {}),
                passed=raw.get("passed", True),
                source=raw.get("source", {"kind": "api_ingest"}),
            )
            run_id = runs.insert_one(doc)
            inserted.append(run_id)
        return Document(inserted=len(inserted), repo_id=repo_doc["_id"])

    @app.route("GET", "/api/v3/tests/{platform}/{namespace}/{repo}")
    def list_tests(request: Request):
        params = request["path_params"]
        absolute = f"{params['platform']}/{params['namespace']}/{params['repo']}"
        q = request["query"]

        filter_ = {"absolute_name": absolute}
        if "branch" in q:
            filter_["branch"] = q["branch"]
        if "test_name" in q:
            filter_["attributes.test_name"] = q["test_name"]
        if "runner" in q:
            filter_["attributes.runner"] = q["runner"]
        if "workflow" in q:
            filter_["attributes.workflow"] = q["workflow"]

        # Query-string timestamps are the one place where we accept naive input
        # and assume UTC — it's a URL convention, documented for the caller.
        # Everywhere else in the stack, naive datetimes fail loudly.
        def _parse_qs_ts(s):
            dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)

        ts_filter = {}
        if "since" in q:
            ts_filter["$gte"] = _parse_qs_ts(q["since"])
        if "until" in q:
            ts_filter["$lte"] = _parse_qs_ts(q["until"])
        if ts_filter:
            filter_["timestamp"] = ts_filter

        hits = runs.find(filter_, sort={"timestamp": 1})

        # If metric= is specified, narrow each run's metrics list.
        metric_name = q.get("metric")
        if metric_name:
            narrowed = []
            for d in hits:
                sub = [m for m in d.get("metrics", []) if m.get("name") == metric_name]
                if not sub:
                    continue
                nd = Document(dict(d.data))
                nd["metrics"] = sub
                narrowed.append(nd)
            hits = Collection(narrowed)

        return Collection(hits)

    # Candidate dimensions the facets endpoint and UI treat as selectable
    # filters. Anything with a single distinct value in a given window is
    # hidden from the UI.
    _FACET_FIELDS = [
        ("branch", lambda d: d.get("branch")),
        ("workflow", lambda d: (d.get("attributes") or {}).get("workflow")),
        ("runner", lambda d: (d.get("attributes") or {}).get("runner")),
    ]

    @app.route("GET", "/api/v3/tests/{platform}/{namespace}/{repo}/facets")
    def facets(request: Request):
        """Distinct values for each known dimension within the given
        filter window. Used by the UI to decide which dimensions to
        surface as radio groups (>1 value) vs hide (1 value).

        Also returns the timestamp span (min / max) of the matched
        runs so the UI can bound its time-range slider.
        """
        params = request["path_params"]
        absolute = f"{params['platform']}/{params['namespace']}/{params['repo']}"
        q = request["query"]
        filter_ = {"absolute_name": absolute}
        # Facets respect existing filters so successive narrowing is
        # consistent: pick a runner, facets recompute against that slice.
        if "branch" in q:
            filter_["branch"] = q["branch"]
        if "workflow" in q:
            filter_["attributes.workflow"] = q["workflow"]
        if "runner" in q:
            filter_["attributes.runner"] = q["runner"]
        if "test_name" in q:
            filter_["attributes.test_name"] = q["test_name"]
        hits = list(runs.find(filter_))
        facets_out: dict = {}
        for name, extract in _FACET_FIELDS:
            vals = set()
            for d in hits:
                v = extract(d)
                if v:
                    vals.add(v)
            if vals:
                facets_out[name] = sorted(vals)
        # Timestamp span.
        tmin = tmax = None
        for d in hits:
            t = d.get("timestamp")
            if t is None:
                continue
            if tmin is None or t < tmin: tmin = t
            if tmax is None or t > tmax: tmax = t
        span = None
        if tmin is not None:
            span = {"min": tmin, "max": tmax}
        return Document(
            facets=facets_out,
            varying=[k for k, v in facets_out.items() if len(v) > 1],
            count=len(hits),
            timestamp_span=span,
        )

    return app
