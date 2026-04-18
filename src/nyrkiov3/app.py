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
    # TestRun is the benchzoo canonical shape plus a few persistence
    # fields (``_id``, ``repo_id``, ``absolute_name``, ``source``).
    # Nothing gets flattened, promoted, or renamed between parser
    # output and storage — the shape parsers emit is the shape we
    # persist. Queries that used to hit top-level ``timestamp`` /
    # ``branch`` / ``git_commit`` now address ``commit.commit_time``
    # / ``commit.ref`` / ``commit.sha``.
    "TestRun": {
        "$id": "TestRun",
        "type": "object",
        "properties": {
            "_id": {"type": "objectid"},
            "repo_id": {"type": "objectid"},
            "absolute_name": {"type": "string"},
            # benchzoo-shape sub-documents:
            "test":    {"type": "object"},   # {test_name, params?, ...}
            "run":     {"type": "object"},   # {passed, ...}
            "env":     {"type": "object"},   # {framework, ...}
            "metrics": {"type": "array", "items": "Metric"},
            "commit":  {"type": "object"},   # {commit_time, sha, ref, repo_url, ...}
            "extra_info": {"type": "object"},
            "sut":     {"type": "object"},
            "source":  {"type": "object"},
        },
        "required": ["absolute_name", "test", "metrics"],
    },
    "IngestPayload": {
        "$id": "IngestPayload",
        "type": "object",
        "properties": {
            "runs": {"type": "array", "items": "IngestRun"},
        },
        "required": ["runs"],
    },
    # External ingest accepts the same benchzoo shape every parser
    # emits. Callers that don't know the commit (most framework
    # parsers) can omit ``commit``; the ingest layer fills ``sha`` /
    # ``ref`` from the request context (path params / webhook event)
    # when available.
    "IngestRun": {
        "$id": "IngestRun",
        "type": "object",
        "properties": {
            "test":    {"type": "object"},
            "run":     {"type": "object"},
            "env":     {"type": "object"},
            "metrics": {"type": "array", "items": "Metric"},
            "commit":  {"type": "object"},
            "extra_info": {"type": "object"},
            "sut":     {"type": "object"},
        },
        "required": ["test", "metrics"],
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


# Small helpers for the public/ingest-url handler. Kept at module level
# because they're pure and unit-testable without spinning up an app.

_GENERIC_STEMS = {"data", "benchmark", "benchmarks", "metrics", "results", "bench"}


_NAT_SPLIT_RE = None  # lazy


def _natural_key(val):
    """Sort key that treats runs of digits as integers so facet values
    like ``"test_2"`` come before ``"test_10"``, and numeric-string
    args (``"64", "512", "4096"``) sort numerically. Non-digit
    segments compare case-insensitively as strings. Works for mixed
    int/str inputs, since attribute values may be either.
    """
    global _NAT_SPLIT_RE
    if _NAT_SPLIT_RE is None:
        import re as _re
        _NAT_SPLIT_RE = _re.compile(r"(\d+)")
    s = str(val)
    parts = _NAT_SPLIT_RE.split(s)
    # (0, "") sentinel prefix so int-valued segments never collide with
    # str-valued segments when a split produces both in the same slot.
    out = []
    for p in parts:
        if p.isdigit():
            out.append((0, int(p)))
        else:
            out.append((1, p.lower()))
    return out


def _parse_repo_url(url):
    """``https://github.com/owner/name`` → ``("gh", "owner", "name")``.
    Anything we don't recognise returns ``None`` — callers use that as
    "skip this row", which is safer than guessing.
    """
    if not url:
        return None
    u = url.rstrip("/")
    for prefix, platform in (
        ("https://github.com/", "gh"),
        ("http://github.com/", "gh"),
        ("github.com/", "gh"),
        ("https://gitlab.com/", "gl"),
    ):
        if u.startswith(prefix):
            parts = u[len(prefix):].split("/")
            if len(parts) >= 2:
                return platform, parts[0], parts[1]
    return None


def _derive_test_name_from_path(path):
    """Old Nyrkiö put the test name in the dashboard URL, not in the
    data file. Replicate that convention by deriving a name from the
    file's path: the filename stem, or the parent directory if the
    stem is a generic placeholder (``devhub/data.json`` → ``devhub``).
    """
    parts = [p for p in path.replace("\\", "/").split("/") if p]
    if not parts:
        return "test"
    fname = parts[-1]
    stem = fname.rsplit(".", 1)[0] if "." in fname else fname
    if stem.lower() in _GENERIC_STEMS and len(parts) >= 2:
        return parts[-2]
    return stem or "test"


def _finalise_run(raw, *, repo_id, absolute, default_source):
    """Attach persistence-only fields to a benchzoo-shaped run dict
    and return the Document ready for ``insert_one``. Doesn't rewrite,
    flatten, or promote anything — the run is stored exactly the
    shape benchzoo emits.

    The only value this function adds: ``repo_id`` / ``absolute_name``
    for the routing decision, and a ``source`` stamp identifying the
    ingest path (parser name, webhook event, api POST, etc.) when the
    caller hasn't attached one already.
    """
    doc = Document(raw)
    doc["repo_id"] = repo_id
    doc["absolute_name"] = absolute
    if "source" not in doc:
        doc["source"] = default_source
    return doc


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
                client = GitHubClient(token)
                workflow = workflow_filename or _detect_workflow(client, owner, repo)
                if not workflow:
                    LOG.warning("no benchmark workflow found on %s/%s", owner, repo)
                    return
                # No parser= pinned: each job's log is sniffed and
                # dispatched via benchzoo.sniff. The workflow may
                # legitimately run different benchmark tools per job.
                summary = ingest_workflow_history(
                    client=client, store=store,
                    owner=owner, repo=repo,
                    workflow_filename=workflow,
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
            t = (d.get("commit") or {}).get("commit_time")
            if not isinstance(t, (int, float)):
                continue
            if entry["latest"] is None or t > entry["latest"]:
                entry["latest"] = t
        # Convert the epoch-int latest to a datetime for the response.
        for entry in by_repo.values():
            if isinstance(entry["latest"], (int, float)):
                entry["latest"] = datetime.datetime.fromtimestamp(entry["latest"], tz=UTC)
        out = sorted(by_repo.values(),
                     key=lambda r: r["latest"] or datetime.datetime.min.replace(tzinfo=UTC),
                     reverse=True)
        return Collection(out)

    @app.route("POST", "/api/v3/public/connect")
    def public_connect(request: Request):
        """Read-only exploration: anyone can point Nyrkiö at a public
        repo; we use the app's own PAT to fetch, no webhook is created.

        Body: ``{"repo": "owner/name", "workflows"?: ["a.yml", ...]}``.
        If ``workflows`` is present, one backfill job is queued per
        filename. Without it we fall through to ``_detect_workflow``
        for a single best-guess pick — preserved for the no-JS /
        scripted caller, but the UI always passes an explicit list now.
        A deprecated scalar ``workflow`` alias is still accepted.
        """
        body = request.get("body") or {}
        repo_str = (body.get("repo") or "").strip().strip("/")
        if "/" not in repo_str:
            raise HTTPError(400, "repo must be 'owner/name'")
        owner, repo = repo_str.split("/", 1)
        token = getattr(app, "github_token", None) or os.environ.get("CLAUDE_GITHUB_PAT", "")
        if not token:
            raise HTTPError(503, "no app-level github token configured")
        workflows = body.get("workflows")
        if not workflows and body.get("workflow"):
            workflows = [body["workflow"]]
        if workflows:
            for wf in workflows:
                _submit_backfill(owner, repo, token, wf)
        else:
            _submit_backfill(owner, repo, token, None)
        return Response(
            body=Document(accepted=True, repo=f"{owner}/{repo}",
                          workflows=workflows or []),
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
            run_id = runs.insert_one(_finalise_run(
                raw, repo_id=repo_doc["_id"], absolute=absolute,
                default_source={"kind": "api_ingest"},
            ))
            inserted.append(run_id)
        return Document(inserted=len(inserted), repo_id=repo_doc["_id"])

    def _insert_runs(platform, namespace, repo, runs_list,
                     default_source=None):
        """Insert a batch of benchzoo-shaped runs under the given repo.
        ``_finalise_run`` just attaches persistence fields; no shape
        rewriting happens."""
        repo_doc = _ensure_repo(platform, namespace, repo)
        absolute = repo_doc["absolute_name"]
        default_source = default_source or {"kind": "parsed"}
        count = 0
        for raw in runs_list:
            runs.insert_one(_finalise_run(
                raw, repo_id=repo_doc["_id"], absolute=absolute,
                default_source=default_source,
            ))
            count += 1
        return absolute, count

    @app.route("POST", "/api/v3/public/ingest-url")
    def public_ingest_url(request: Request):
        """Fetch a single file from a public GitHub repo, content-sniff
        it, dispatch to the matching benchzoo parser, and ingest.

        Routing: each parsed run is filed under the repo its
        ``commit.repo_url`` points at (the Nyrkiö-v1 case, where a
        file in repo X can carry measurements about repo Y — see
        tigerbeetle/devhubdb). For any other format, the parser
        doesn't know the target repo, so we fall back to the
        ``source_repo`` — "the data lives here, so probably about
        here" is the only defensible default.
        """
        import urllib.request
        from benchzoo import sniff as _sniff_content
        from benchzoo.parsers import find_parser

        body = request.get("body") or {}
        source_repo = (body.get("source_repo") or "").strip().strip("/")
        path = (body.get("path") or "").strip().lstrip("/")
        if "/" not in source_repo or not path:
            raise HTTPError(400, "expected {source_repo: 'owner/name', path: 'dir/file.json'}")
        default_target = _parse_repo_url(f"https://github.com/{source_repo}")
        if default_target is None:
            raise HTTPError(400, f"bad source_repo: {source_repo!r}")
        # Test name: caller can override; otherwise derive from the
        # file's path the same way old Nyrkiö dashboard URLs worked.
        test_name = (body.get("test_name") or "").strip() or _derive_test_name_from_path(path)
        url = f"https://raw.githubusercontent.com/{source_repo}/HEAD/{path}"
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                blob = resp.read().decode("utf-8")
        except Exception as e:
            raise HTTPError(502, f"fetch failed: {e}")
        framework = _sniff_content(blob)
        if framework is None:
            raise HTTPError(
                400,
                "couldn't identify benchmark format — sniff returned no match. "
                "Supported formats are listed in benchzoo.parsers.PARSERS."
            )
        try:
            parser_mod = find_parser(framework)
        except (KeyError, ValueError) as e:
            raise HTTPError(
                500,
                f"sniff returned {framework!r} but no single parser module "
                f"matched: {e}"
            )
        try:
            runs_flat = parser_mod.parse(blob)
        except Exception as e:
            raise HTTPError(400, f"parse ({framework}) failed: {e}")
        # Route each run to the repo its commit claims — or, if the
        # parser didn't extract a commit, back to the source repo.
        # The URL-derived test name is injected if the parser didn't
        # set one (common for v1 files; gbench always sets one).
        by_repo: dict[tuple[str, str, str], list[dict]] = {}
        for run in runs_flat:
            commit = run.get("commit") or {}
            target = _parse_repo_url(commit.get("repo_url") or "") or default_target
            test = run.setdefault("test", {})
            if test_name and not test.get("test_name"):
                test["test_name"] = test_name
            by_repo.setdefault(target, []).append(run)
        if not by_repo:
            raise HTTPError(400, "no rows after parsing — file may be empty or malformed")
        results = []
        for (platform, namespace, repo), runs_list in by_repo.items():
            absolute, count = _insert_runs(
                platform, namespace, repo, runs_list,
                default_source={"kind": "public_ingest_url"},
            )
            results.append({"absolute_name": absolute, "inserted": count})
        # Primary target = the repo with the most rows. Tigerbeetle-style
        # files almost always have a single target; the multi-repo case
        # is legal but rare.
        results.sort(key=lambda r: r["inserted"], reverse=True)
        primary = results[0]
        target_short = primary["absolute_name"].split("/", 1)[1]  # strip gh/
        return Response(
            body=Document(target_repo=target_short, inserted=primary["inserted"],
                          test_name=test_name, all_targets=results),
            status=200,
        )

    @app.route("GET", "/api/v3/tests/{platform}/{namespace}/{repo}")
    def list_tests(request: Request):
        params = request["path_params"]
        absolute = f"{params['platform']}/{params['namespace']}/{params['repo']}"
        q = request["query"]

        filter_ = {"absolute_name": absolute}
        _apply_facet_filters(filter_, q)

        # Query-string timestamps are the one place where we accept naive input
        # and assume UTC — it's a URL convention, documented for the caller.
        # Everywhere else in the stack, naive datetimes fail loudly.
        def _parse_qs_ts(s):
            dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)

        # Time range narrows on commit time — the authoritative
        # ordering signal. A UI caller passes ISO-8601 strings; we
        # compare against ``commit.commit_time`` which is stored as
        # an epoch int. Convert once at the boundary.
        ts_filter = {}
        if "since" in q:
            ts_filter["$gte"] = int(_parse_qs_ts(q["since"]).timestamp())
        if "until" in q:
            ts_filter["$lte"] = int(_parse_qs_ts(q["until"]).timestamp())
        if ts_filter:
            filter_["commit.commit_time"] = ts_filter

        hits = runs.find(filter_, sort={"commit.commit_time": 1})

        # metric= narrows each run's metrics list. Repeated params
        # (``?metric=a&metric=b``) keep any matching name; missing →
        # keep all metrics untouched.
        metric_set = {v for v in q.getall("metric") if v}
        if metric_set:
            narrowed = []
            for d in hits:
                sub = [m for m in d.get("metrics", [])
                       if m.get("name") in metric_set]
                if not sub:
                    continue
                nd = Document(dict(d.data))
                nd["metrics"] = sub
                narrowed.append(nd)
            hits = Collection(narrowed)

        return Collection(hits)

    # Known dimensions that live at well-defined nested paths in a
    # benchzoo-shaped run. Every other facet key is assumed to be a
    # free-form benchmark parameter under ``test.params.*`` — that's
    # where parsers write threads / args / vus / clients / iterations
    # / and any future knob.
    _SCALAR_FACETS = {
        "branch":    "commit.ref",
        "test_name": "test.test_name",
        "runner":    "run.runner",
        "workflow":  "run.workflow",
    }

    def _filter_path(key):
        return _SCALAR_FACETS.get(key, f"test.params.{key}")

    def _apply_facet_filters(filter_, q):
        """Honour repeated-param multi-value filters. ``?branch=main``
        is a single-value equality; ``?branch=main&branch=develop``
        becomes ``$in``. Empty values (``?branch=``) are skipped —
        that's the UI signal for 'all'. ``metric`` is handled by the
        caller because it narrows each run's ``metrics[]`` list rather
        than filtering runs on a scalar field."""
        for key in q:
            if key == "metric":
                continue
            if key in ("since", "until"):
                continue
            vals = [v for v in q.getall(key) if v]
            if not vals:
                continue
            filter_[_filter_path(key)] = (vals[0] if len(vals) == 1
                                          else {"$in": vals})

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
        _apply_facet_filters(filter_, q)
        hits = list(runs.find(filter_))
        dim_values: dict[str, set] = {}

        def _bump(key, val):
            if val is None or val == "":
                return
            dim_values.setdefault(key, set()).add(val)

        for d in hits:
            test = d.get("test") or {}
            run = d.get("run") or {}
            commit = d.get("commit") or {}
            _bump("test_name", test.get("test_name"))
            _bump("branch",    commit.get("ref"))
            _bump("runner",    run.get("runner"))
            _bump("workflow",  run.get("workflow"))
            # Free-form parameters the parser extracted (threads,
            # args, clients, vus, …) — any key under ``test.params``
            # is a candidate facet, and the filter path is built
            # symmetrically in ``_filter_path``.
            for k, v in (test.get("params") or {}).items():
                _bump(k, v)
            # Metric is list-valued: surface its names.
            for m in d.get("metrics") or []:
                _bump("metric", m.get("name"))

        facets_out = {
            k: sorted(dim_values[k], key=_natural_key)
            for k in sorted(dim_values)
        }

        # Timestamp span — commit time is the authoritative ordering
        # signal, stored as an epoch int under ``commit.commit_time``.
        # Convert to ISO-friendly ``{min, max}`` datetimes for the UI.
        tmin = tmax = None
        for d in hits:
            t = (d.get("commit") or {}).get("commit_time")
            if not isinstance(t, (int, float)):
                continue
            if tmin is None or t < tmin: tmin = t
            if tmax is None or t > tmax: tmax = t
        span = None
        if tmin is not None:
            span = {
                "min": datetime.datetime.fromtimestamp(tmin, tz=UTC),
                "max": datetime.datetime.fromtimestamp(tmax, tz=UTC),
            }
        return Document(
            facets=facets_out,
            varying=[k for k, v in facets_out.items() if len(v) > 1],
            count=len(hits),
            timestamp_span=span,
        )

    return app
