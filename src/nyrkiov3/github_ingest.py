"""GitHub Actions benchmark ingester.

Walks a repo's workflow runs, downloads each run's job logs (or
artifacts), parses them via benchzoo, and writes the resulting runs to
a store. Works for both manual / CLI-driven backfills and the webhook
path (``handle_workflow_run_event`` — same code, different trigger).

Authentication: expects a GitHub token. The token is passed in
explicitly rather than read from the environment so the same ingester
is reusable in tests with a mocked HTTP client.

Parser selection is registry-based: the caller supplies a mapping of
``workflow filename -> benchzoo parser module``. The ingester slices the
log to the right region (via a configurable start / end line marker
pair, usually step names from the workflow YAML) and hands it to the
parser. A per-call default, ``default_parser``, applies when no
per-workflow entry matches.

No artifact handling yet — UnoDB emits plain stdout — but the module
is shaped so ``fetch_artifact_zip`` can slot in later without
disturbing the public surface.
"""
from __future__ import annotations

import datetime as _dt
import io
import json
import logging
import re
import urllib.error
import urllib.request
import zipfile

from purejson import Document
from extjson import utcnow


LOG = logging.getLogger("nyrkiov3.github_ingest")

GITHUB_API = "https://api.github.com"
API_VERSION = "2022-11-28"


class _Redirect(Exception):
    """Internal signal: the redirect handler saw a 3xx and is done."""
    def __init__(self, location: str):
        self.location = location


# ----------------------------------------------------------------------------
# HTTP layer
# ----------------------------------------------------------------------------

class GitHubClient:
    """Tiny GitHub REST client. Only the endpoints we actually call."""

    def __init__(self, token: str, *, user_agent: str = "nyrkio-sync/0.1"):
        if not token:
            raise ValueError("a GitHub token is required")
        self._token = token
        self._ua = user_agent

    def _request(self, path_or_url: str, *, accept: str = "application/vnd.github+json",
                 binary: bool = False):
        url = path_or_url if path_or_url.startswith("http") else GITHUB_API + path_or_url
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {self._token}",
            "Accept": accept,
            "X-GitHub-Api-Version": API_VERSION,
            "User-Agent": self._ua,
        })
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = resp.read()
            if binary:
                return data
            return json.loads(data.decode("utf-8"))

    def list_workflow_runs(self, owner: str, repo: str, workflow_filename: str,
                           *, branch: str | None = None,
                           status: str = "success",
                           per_page: int = 100,
                           max_pages: int = 10) -> list[dict]:
        """Return workflow runs, newest first, up to max_pages * per_page."""
        out: list[dict] = []
        base = f"/repos/{owner}/{repo}/actions/workflows/{workflow_filename}/runs"
        qs = f"?per_page={per_page}"
        if status:
            qs += f"&status={status}"
        if branch:
            qs += f"&branch={branch}"
        for page in range(1, max_pages + 1):
            data = self._request(f"{base}{qs}&page={page}")
            runs = data.get("workflow_runs", [])
            if not runs:
                break
            out.extend(runs)
            if len(runs) < per_page:
                break
        return out

    def list_jobs(self, owner: str, repo: str, run_id: int) -> list[dict]:
        data = self._request(f"/repos/{owner}/{repo}/actions/runs/{run_id}/jobs")
        return data.get("jobs", [])

    def get_job_log(self, owner: str, repo: str, job_id: int) -> str:
        # The logs endpoint 302s to a signed S3 URL. We can't follow the
        # redirect automatically because urllib would carry the
        # ``Authorization: Bearer ...`` header forward and S3 rejects
        # requests that don't match its query-string signing. So: take
        # the redirect manually, then fetch the Location URL with no
        # auth header at all.
        url = GITHUB_API + f"/repos/{owner}/{repo}/actions/jobs/{job_id}/logs"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {self._token}",
            "Accept": "*/*",
            "X-GitHub-Api-Version": API_VERSION,
            "User-Agent": self._ua,
        })

        class _NoRedirect(urllib.request.HTTPRedirectHandler):
            def http_error_302(self, req, fp, code, msg, headers):
                raise _Redirect(headers.get("Location"))
            http_error_301 = http_error_302
            http_error_303 = http_error_302
            http_error_307 = http_error_302
            http_error_308 = http_error_302

        opener = urllib.request.build_opener(_NoRedirect)
        try:
            with opener.open(req, timeout=60) as resp:
                raw = resp.read()
        except _Redirect as r:
            with urllib.request.urlopen(r.location, timeout=60) as resp2:
                raw = resp2.read()
        return raw.decode("utf-8", errors="replace")

    def get_commit(self, owner: str, repo: str, sha: str) -> dict:
        return self._request(f"/repos/{owner}/{repo}/commits/{sha}")


# ----------------------------------------------------------------------------
# Log slicing
# ----------------------------------------------------------------------------

# GH Actions log lines carry an ISO-8601 timestamp prefix. We don't strip
# it here — parsers accept prefixed output — but we do recognise the
# ``##[group]`` / ``##[endgroup]`` markers and the plain "Running" lines
# emitted by matrix step names so callers can slice to the right region.

_STEP_GROUP = re.compile(r"^(?:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s+)?##\[group\](.*)$")
_STEP_ENDGROUP = re.compile(r"^(?:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s+)?##\[endgroup\]\s*$")


def slice_log(log: str, *, step_name: str | None = None) -> str:
    """Return the portion of the log inside ``##[group]step_name``/``##[endgroup]``.

    If ``step_name`` is None, returns the full log unchanged — parsers
    that are robust against noise can consume the whole thing.
    """
    if step_name is None:
        return log
    lines = log.splitlines(keepends=True)
    out: list[str] = []
    inside = False
    for line in lines:
        m = _STEP_GROUP.match(line.rstrip("\r\n"))
        if m:
            inside = (step_name in m.group(1))
            continue
        if _STEP_ENDGROUP.match(line.rstrip("\r\n")):
            if inside:
                return "".join(out)
            continue
        if inside:
            out.append(line)
    return "".join(out)


# ----------------------------------------------------------------------------
# Ingest orchestration
# ----------------------------------------------------------------------------

def _commit_sub_doc(commit_info: dict) -> dict:
    """Build a v2-shaped `commit` dict from a GitHub commit API payload."""
    commit = commit_info.get("commit", {})
    author = commit.get("author", {})
    committer = commit.get("committer", {})
    commit_time = None
    for src in (committer, author):
        ts_str = src.get("date")
        if ts_str:
            try:
                commit_time = int(_dt.datetime.fromisoformat(
                    ts_str.replace("Z", "+00:00")).timestamp())
                break
            except ValueError:
                pass
    out = {
        "sha": commit_info.get("sha", ""),
        "short_sha": (commit_info.get("sha") or "")[:7],
        "message": (commit.get("message") or "").split("\n", 1)[0],
        "author": author.get("name", ""),
    }
    if commit_time is not None:
        out["commit_time"] = commit_time
    return out


def ingest_workflow_run(
    *,
    client: GitHubClient,
    store,
    owner: str,
    repo: str,
    run: dict,
    parsers: dict[str, object] | None = None,
    default_parser=None,
    step_name: str | None = None,
    job_filter=None,
) -> int:
    """Pull logs for every successful job in ``run``, parse, insert.

    Returns the number of benchmark runs inserted.

    Parameters:
    - ``parsers``: optional ``{workflow_filename: parser_module}`` map.
    - ``default_parser``: used when ``parsers`` doesn't match. One of
      ``parsers`` or ``default_parser`` must be set.
    - ``step_name``: if given, slice the log to that ``##[group]``
      region before parsing (tight-coupling the parser to just the
      benchmark step). Otherwise the parser sees the entire job log.
    - ``job_filter``: optional callable ``(job_dict) -> bool`` to skip
      jobs whose name / os doesn't match.
    """
    workflow_filename = (run.get("path") or "").split("/")[-1]
    parser = None
    if parsers and workflow_filename in parsers:
        parser = parsers[workflow_filename]
    elif default_parser is not None:
        parser = default_parser
    if parser is None:
        raise ValueError(
            f"no parser configured for workflow {workflow_filename!r}")

    sha = run.get("head_sha", "")
    try:
        commit_info = client.get_commit(owner, repo, sha) if sha else {}
    except urllib.error.HTTPError as e:
        LOG.warning("commit %s not fetchable (%s); skipping run %s", sha, e, run.get("id"))
        return 0
    commit = _commit_sub_doc(commit_info) if commit_info else {
        "sha": sha, "short_sha": sha[:7]
    }

    # Timestamp used on the stored run: canonical = commit time if we
    # know it, else the run's created_at.
    if "commit_time" in commit:
        ts = _dt.datetime.fromtimestamp(commit["commit_time"], tz=_dt.timezone.utc)
    else:
        ts_str = run.get("created_at", "").replace("Z", "+00:00")
        try:
            ts = _dt.datetime.fromisoformat(ts_str)
        except ValueError:
            ts = utcnow()

    jobs = client.list_jobs(owner, repo, run["id"])
    absolute = f"gh/{owner}/{repo}"
    repos = store.collection("repos")
    runs_coll = store.collection("test_runs")

    # Ensure the repo doc exists.
    if repos.find_one({"absolute_name": absolute}) is None:
        repos.insert_one(Document(
            platform="gh", namespace=owner, repo=repo,
            absolute_name=absolute, installed_at=utcnow(),
        ))
    repo_doc = repos.find_one({"absolute_name": absolute})

    inserted = 0
    for job in jobs:
        if job.get("conclusion") != "success":
            continue
        if job_filter is not None and not job_filter(job):
            continue
        try:
            log = client.get_job_log(owner, repo, job["id"])
        except urllib.error.HTTPError as e:
            LOG.warning("log fetch failed for job %s (%s); skipping", job["id"], e)
            continue
        sliced = slice_log(log, step_name=step_name)
        parsed = parser.parse(sliced)
        if not parsed:
            LOG.info("job %s produced no parsed runs", job["id"])
            continue

        for entry in parsed:
            test_name = entry.get("test", {}).get("test_name") or ""
            metrics = entry.get("metrics", [])
            if not test_name or not metrics:
                continue
            attrs = {
                "test_name": test_name,
                "runner": job.get("name", ""),
                "workflow": workflow_filename,
            }
            extra_info = dict(entry.get("extra_info") or {})
            if "sut" in entry:
                extra_info["sut"] = entry["sut"]
            if "env" in entry:
                extra_info["env"] = entry["env"]
            runs_coll.insert_one(Document(
                repo_id=repo_doc["_id"],
                absolute_name=absolute,
                branch=run.get("head_branch", "main"),
                git_commit=sha,
                timestamp=ts,
                attributes=attrs,
                metrics=metrics,
                commit=commit,
                extra_info=extra_info,
                passed=entry.get("run", {}).get("passed", True),
                source={"kind": "github_actions",
                        "run_id": run["id"],
                        "job_id": job["id"],
                        "workflow": workflow_filename},
            ))
            inserted += 1
    return inserted


def ingest_workflow_history(
    *,
    client: GitHubClient,
    store,
    owner: str,
    repo: str,
    workflow_filename: str,
    parser=None,
    parsers: dict[str, object] | None = None,
    step_name: str | None = None,
    job_filter=None,
    branch: str | None = None,
    max_pages: int = 5,
) -> dict:
    """Walk workflow history newest-first and ingest each successful run."""
    runs = client.list_workflow_runs(
        owner, repo, workflow_filename,
        branch=branch, status="success", max_pages=max_pages,
    )
    inserted_total = 0
    seen = 0
    for wrun in runs:
        seen += 1
        try:
            n = ingest_workflow_run(
                client=client, store=store, owner=owner, repo=repo,
                run=wrun, parsers=parsers, default_parser=parser,
                step_name=step_name, job_filter=job_filter,
            )
        except Exception as e:
            LOG.exception("ingest failed for run %s: %s", wrun.get("id"), e)
            continue
        inserted_total += n
        LOG.info("run %s @ %s: +%d benchmarks", wrun["id"], wrun.get("head_sha", "")[:7], n)
    return {"runs_seen": seen, "benchmarks_inserted": inserted_total}


def handle_workflow_run_event(
    *,
    client: GitHubClient,
    store,
    payload: dict,
    parsers: dict[str, object] | None = None,
    default_parser=None,
    step_name: str | None = None,
    job_filter=None,
) -> int:
    """Webhook entry point: a ``workflow_run`` event with action=completed.

    Validation of the HMAC signature is the caller's responsibility —
    this function assumes the payload is already trusted.
    """
    if payload.get("action") != "completed":
        return 0
    run = payload.get("workflow_run") or {}
    if run.get("conclusion") != "success":
        return 0
    repo_block = payload.get("repository") or {}
    owner = (repo_block.get("owner") or {}).get("login", "")
    repo = repo_block.get("name", "")
    if not owner or not repo:
        raise ValueError("payload missing repository owner/name")
    return ingest_workflow_run(
        client=client, store=store, owner=owner, repo=repo, run=run,
        parsers=parsers, default_parser=default_parser,
        step_name=step_name, job_filter=job_filter,
    )
