"""Nyrkiö v3 app: ingest + read endpoints, benchzoo-shaped payload,
FerretDB-compatible store interface. Schemas in SCHEMAS are authoritative."""
from __future__ import annotations

import datetime
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from purejson import Document, Collection
from extjson import ObjectId, dumps, utcnow, parse_date, to_utc, UTC
from jsonee import JsonEE, Request, Response, HTTPError, InMemoryStore

from . import auth as _auth


LOG = logging.getLogger("nyrkiov3.app")


SESSION_COOKIE = "nyrkio_session"
OAUTH_STATE_COOKIE = "nyrkio_oauth_state"


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


def _parse_cookies(headers):
    raw = (headers or {}).get("cookie", "") or ""
    out = {}
    for chunk in raw.split(";"):
        if "=" not in chunk:
            continue
        k, v = chunk.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _current_user(request, store, secret):
    """Return the user document for this request, or None."""
    cookies = _parse_cookies(request.get("headers") or {})
    cookie = cookies.get(SESSION_COOKIE)
    if not cookie or not secret:
        return None
    try:
        payload = _auth.verify_session(cookie, secret)
    except _auth.AuthError:
        return None
    users = store.collection("users")
    return users.find_one({"github_id": payload.get("github_id")})


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

    # Auth / OAuth configuration. These are populated by the caller
    # (demo_server, production wiring) before serving requests; missing
    # values cause the OAuth endpoints to 503 politely so local dev
    # without a registered GitHub App still works.
    # Public-facing base URL Nyrkiö is served from. Hardcoded so we
    # can register the OAuth callback and webhook URLs with GitHub once
    # and forget. For local dev override with NYRKIO_BASE_URL.
    app.auth_config = {
        "client_id": os.environ.get("NYRKIO_GITHUB_CLIENT_ID", ""),
        "client_secret": os.environ.get("NYRKIO_GITHUB_CLIENT_SECRET", ""),
        "session_secret": os.environ.get("NYRKIO_SESSION_SECRET", ""),
        "base_url": os.environ.get("NYRKIO_BASE_URL", "https://nyrkio.com"),
    }

    @app.route("GET", "/api/v3/config")
    def get_config(request: Request):
        # Expose which auth flows are configured so the landing page
        # can grey out the "Sign in" button when we have no client_id.
        cfg = dict(app.config)
        cfg["auth_enabled"] = bool(
            app.auth_config["client_id"] and app.auth_config["session_secret"])
        return Document(cfg)

    # ------ auth: /login, /oauth/callback, /logout, /api/v3/me ---------

    def _require_auth():
        ac = app.auth_config
        if not ac["client_id"] or not ac["client_secret"] or not ac["session_secret"]:
            raise HTTPError(503, "oauth not configured on this instance")

    @app.route("GET", "/login")
    def login(request: Request):
        _require_auth()
        state = _auth.new_state()
        base = app.auth_config["base_url"] or ""
        redirect_uri = f"{base}/oauth/callback"
        url = _auth.authorize_url(
            app.auth_config["client_id"], redirect_uri, state)
        # Stash state in a short-lived cookie so the callback can verify.
        return Response(
            body=None, status=302,
            headers={
                "location": url,
                "set-cookie": f"{OAUTH_STATE_COOKIE}={state}; Path=/; HttpOnly; Max-Age=600; SameSite=Lax",
            },
        )

    @app.route("GET", "/oauth/callback")
    def oauth_callback(request: Request):
        _require_auth()
        q = request["query"]
        code = q.get("code", "")
        state = q.get("state", "")
        cookies = _parse_cookies(request.get("headers") or {})
        if not code or not state or cookies.get(OAUTH_STATE_COOKIE) != state:
            raise HTTPError(400, "oauth state mismatch or missing code")
        ac = app.auth_config
        base = ac["webhook_base_url"] or ""
        redirect_uri = f"{base}/oauth/callback"
        try:
            token = _auth.exchange_code(
                ac["client_id"], ac["client_secret"], code, redirect_uri)
            gh_user = _auth.fetch_user(token)
        except Exception as e:
            raise HTTPError(502, f"github oauth failed: {e}")
        users = store.collection("users")
        existing = users.find_one({"github_id": gh_user["id"]})
        user_doc = Document(
            github_id=gh_user["id"],
            username=gh_user.get("login", ""),
            name=gh_user.get("name") or "",
            avatar_url=gh_user.get("avatar_url") or "",
            email=gh_user.get("email") or "",
            # Access token held plaintext for this prototype; encrypt
            # with a cold key in production.
            access_token=token,
            updated_at=utcnow(),
        )
        if existing is None:
            user_doc["created_at"] = utcnow()
            users.insert_one(user_doc)
        else:
            # Re-insert as a "latest snapshot"; the store has no
            # update-in-place. Keep the old _id so sessions pointing at
            # it still resolve via github_id lookup.
            user_doc["_id"] = existing["_id"]
            user_doc["created_at"] = existing.get("created_at", utcnow())
            # Purge the stale copy so find_one returns the fresh one.
            users._docs.data[:] = [
                d for d in users._docs.data
                if d.get("github_id") != gh_user["id"]
            ]
            users.insert_one(user_doc)
        session = _auth.sign_session(
            {"github_id": gh_user["id"], "username": gh_user.get("login", "")},
            ac["session_secret"])
        return Response(
            body=None, status=302,
            headers={
                "location": f"{app.auth_config['base_url']}/",
                "set-cookie": [
                    f"{SESSION_COOKIE}={session}; Path=/; HttpOnly; Max-Age={30*86400}; SameSite=Lax",
                    f"{OAUTH_STATE_COOKIE}=; Path=/; Max-Age=0",
                ],
            },
        )

    @app.route("GET", "/logout")
    def logout(request: Request):
        return Response(
            body=None, status=302,
            headers={
                "location": f"{app.auth_config['base_url']}/",
                "set-cookie": f"{SESSION_COOKIE}=; Path=/; Max-Age=0",
            },
        )

    @app.route("GET", "/api/v3/me")
    def me(request: Request):
        u = _current_user(request, store, app.auth_config["session_secret"])
        if not u:
            raise HTTPError(401, "not signed in")
        # Don't leak the access token to the browser.
        return Document(
            github_id=u["github_id"],
            username=u["username"],
            name=u.get("name", ""),
            avatar_url=u.get("avatar_url", ""),
        )

    @app.route("GET", "/api/v3/me/repos")
    def my_repos(request: Request):
        u = _current_user(request, store, app.auth_config["session_secret"])
        if not u:
            raise HTTPError(401, "not signed in")
        # Fetch user's repos via their token; surface name + basics only.
        import urllib.request, urllib.error, json as _json
        req = urllib.request.Request(
            "https://api.github.com/user/repos?per_page=100&sort=pushed",
            headers={
                "Authorization": f"Bearer {u['access_token']}",
                "Accept": "application/vnd.github+json",
                "User-Agent": "nyrkio/0.1",
            })
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = _json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise HTTPError(502, f"github repos: {e}")
        out = []
        for r in raw:
            out.append({
                "full_name": r.get("full_name"),
                "private": r.get("private"),
                "description": r.get("description") or "",
                "has_actions": True,  # we'd check /actions but keep it simple for now
            })
        return Collection(out)

    def _submit_backfill(owner: str, repo: str, token: str,
                        workflow_filename: str | None = None):
        """Queue a best-effort backfill for the given repo."""
        def work():
            try:
                from .github_ingest import GitHubClient, ingest_workflow_history
                from benchzoo.parsers import google_benchmark_text
                client = GitHubClient(token)
                workflow = workflow_filename or _detect_workflow(client, owner, repo)
                if not workflow:
                    LOG.warning("no benchmark workflow found on %s/%s", owner, repo)
                    return
                summary = ingest_workflow_history(
                    client=client, store=store,
                    owner=owner, repo=repo,
                    workflow_filename=workflow,
                    parser=google_benchmark_text,
                )
                LOG.info("backfill %s/%s (%s): %s", owner, repo, workflow, summary)
            except Exception:
                LOG.exception("backfill failed for %s/%s", owner, repo)
        app.background.submit(work)

    def _detect_workflow(client, owner: str, repo: str) -> str | None:
        """Find a plausibly-benchmark workflow by filename substring."""
        import urllib.request, json as _json
        req = urllib.request.Request(
            f"https://api.github.com/repos/{owner}/{repo}/actions/workflows",
            headers={"Authorization": f"Bearer {client._token}",
                     "Accept": "application/vnd.github+json",
                     "User-Agent": "nyrkio/0.1"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = _json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None
        # Prefer files with "bench" in the path; fall back to "benchmarks.yml".
        candidates = data.get("workflows", [])
        for w in candidates:
            path = (w.get("path") or "").lower()
            if "bench" in path:
                return path.split("/")[-1]
        return "benchmarks.yml"

    @app.route("POST", "/api/v3/me/repos/{owner}/{repo}/connect")
    def connect_repo(request: Request):
        u = _current_user(request, store, app.auth_config["session_secret"])
        if not u:
            raise HTTPError(401, "not signed in")
        params = request["path_params"]
        owner, repo = params["owner"], params["repo"]
        body = request.get("body") or {}
        workflow = body.get("workflow") or None
        # We could also create a GitHub webhook here so future runs
        # stream in live; for now just a one-shot backfill. The webhook
        # hook-up lives in /connect-webhook as a follow-up.
        _submit_backfill(owner, repo, u["access_token"], workflow)
        return Response(
            body=Document(accepted=True, repo=f"{owner}/{repo}"),
            status=202,
        )

    @app.route("GET", "/api/v3/public/dashboards")
    def public_dashboards(request: Request):
        """All repos with data in the store, with aggregated stats.
        Surfaced on the landing page so visitors can click through
        without needing to know which repos are available.

        Privacy: returns everything in the store today — fine while
        the store is public-only. When we add private repos, this
        needs to filter by visibility (or per-user auth)."""
        by_repo: dict = {}
        for d in runs.find({}):
            abs_name = d.get("absolute_name", "")
            if not abs_name:
                continue
            entry = by_repo.setdefault(abs_name, {
                "absolute_name": abs_name,
                "run_count": 0,
                "latest": None,
            })
            entry["run_count"] += 1
            ts = d.get("timestamp")
            if ts is None:
                continue
            # Timestamps are datetime; compare directly.
            if entry["latest"] is None or ts > entry["latest"]:
                entry["latest"] = ts
        out = sorted(by_repo.values(),
                     key=lambda r: r["latest"] or datetime.datetime.min.replace(tzinfo=UTC),
                     reverse=True)
        return Collection(out)

    @app.route("POST", "/api/v3/public/connect")
    def public_connect(request: Request):
        """Read-only exploration: anyone can point Nyrkiö at a public
        repo; we use the app's own PAT to fetch, no webhook is created."""
        body = request.get("body") or {}
        repo_str = (body.get("repo") or "").strip().strip("/")
        workflow = body.get("workflow") or None
        if "/" not in repo_str:
            raise HTTPError(400, "repo must be 'owner/name'")
        owner, repo = repo_str.split("/", 1)
        token = getattr(app, "github_token", None) or os.environ.get("CLAUDE_GITHUB_PAT", "")
        if not token:
            raise HTTPError(503, "no app-level github token configured")
        _submit_backfill(owner, repo, token, workflow)
        return Response(
            body=Document(accepted=True, repo=f"{owner}/{repo}"),
            status=202,
        )

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
