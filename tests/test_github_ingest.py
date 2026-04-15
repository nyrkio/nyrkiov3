"""Tests for the GitHub ingester with an in-process fake client."""
from __future__ import annotations

import pathlib

import pytest

from benchzoo.parsers import google_benchmark_text
from jsonee import InMemoryStore
from nyrkiov3.github_ingest import (
    GitHubClient,
    _commit_sub_doc,
    handle_workflow_run_event,
    ingest_workflow_run,
    slice_log,
)


BENCHZOO_DATA = pathlib.Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# A tiny in-memory "GitHub" — implements the 3 methods the ingester calls.
# ---------------------------------------------------------------------------

class FakeGitHub:
    def __init__(self, *, runs, jobs, logs, commits):
        self._runs = runs
        self._jobs = jobs  # run_id -> [job]
        self._logs = logs  # job_id -> str
        self._commits = commits  # sha -> commit dict

    def list_workflow_runs(self, owner, repo, workflow, **kwargs):
        return list(self._runs)

    def list_jobs(self, owner, repo, run_id):
        return list(self._jobs.get(run_id, []))

    def get_job_log(self, owner, repo, job_id):
        return self._logs[job_id]

    def get_commit(self, owner, repo, sha):
        return self._commits[sha]


# A tiny google-benchmark text blob with a GH Actions timestamp prefix.
_SAMPLE_LOG = """\
2026-04-14T08:00:00.0000000Z ##[group]Run benchmarks
2026-04-14T08:00:01.0000000Z Running ./sample_benchmark
2026-04-14T08:00:01.0000000Z Run on (2 X 2872.85 MHz CPU s)
2026-04-14T08:00:01.0000000Z ---------------------------------------------------
2026-04-14T08:00:01.0000000Z Benchmark                Time             CPU   Iterations
2026-04-14T08:00:01.0000000Z ---------------------------------------------------
2026-04-14T08:00:02.0000000Z benchmark1       2150 ms      0.043 ms            1
2026-04-14T08:00:03.0000000Z benchmark2      0.004 ms      0.004 ms      169190
2026-04-14T08:00:04.0000000Z ##[endgroup]
"""


def _commit_payload(sha, message, author_name, iso_date):
    return {
        "sha": sha,
        "commit": {
            "message": message,
            "author": {"name": author_name, "date": iso_date},
            "committer": {"name": author_name, "date": iso_date},
        },
    }


@pytest.fixture
def fake_client():
    return FakeGitHub(
        runs=[{
            "id": 1001, "head_sha": "a" * 40,
            "head_branch": "main",
            "path": ".github/workflows/benchmarks.yml",
            "created_at": "2026-04-14T08:00:00Z",
            "conclusion": "success",
        }],
        jobs={1001: [
            {"id": 9001, "name": "benchmark", "conclusion": "success"},
        ]},
        logs={9001: _SAMPLE_LOG},
        commits={"a" * 40: _commit_payload(
            "a" * 40, "optimize the hot loop",
            "Anna Virtanen", "2026-04-14T07:55:00Z",
        )},
    )


# ---------------------------------------------------------------------------

def test_commit_sub_doc_shape():
    payload = _commit_payload("a" * 40, "fix foo", "Ben", "2026-04-14T07:55:00Z")
    c = _commit_sub_doc(payload)
    assert c["sha"] == "a" * 40
    assert c["short_sha"] == "a" * 7
    assert c["message"] == "fix foo"
    assert c["author"] == "Ben"
    assert isinstance(c["commit_time"], int)


def test_commit_sub_doc_multiline_message_keeps_first_line_only():
    payload = _commit_payload("b" * 40, "subject\n\nbody", "X", "2026-01-01T00:00:00Z")
    assert _commit_sub_doc(payload)["message"] == "subject"


def test_slice_log_to_step():
    sliced = slice_log(_SAMPLE_LOG, step_name="Run benchmarks")
    assert "Running ./sample_benchmark" in sliced
    assert "##[group]" not in sliced
    assert "##[endgroup]" not in sliced


def test_slice_log_no_step_returns_full():
    assert slice_log(_SAMPLE_LOG, step_name=None) is _SAMPLE_LOG


def test_slice_log_step_not_found_returns_empty():
    assert slice_log(_SAMPLE_LOG, step_name="Nonexistent") == ""


def test_ingest_one_run_inserts_benchmarks(fake_client):
    store = InMemoryStore()
    n = ingest_workflow_run(
        client=fake_client, store=store,
        owner="unodb-dev", repo="unodb",
        run=fake_client._runs[0],
        default_parser=google_benchmark_text,
        step_name="Run benchmarks",
    )
    assert n == 2
    runs = list(store.collection("test_runs").find({}))
    assert len(runs) == 2
    # Commit metadata propagated to each run's top-level `commit`.
    for r in runs:
        assert r["commit"]["short_sha"] == "a" * 7
        assert r["commit"]["message"] == "optimize the hot loop"
        assert r["commit"]["author"] == "Anna Virtanen"
        assert "commit_time" in r["commit"]
        assert r["source"]["kind"] == "github_actions"
        assert r["source"]["run_id"] == 1001


def test_ingest_skips_failed_jobs(fake_client):
    fake_client._jobs[1001][0]["conclusion"] = "failure"
    store = InMemoryStore()
    n = ingest_workflow_run(
        client=fake_client, store=store,
        owner="unodb-dev", repo="unodb",
        run=fake_client._runs[0],
        default_parser=google_benchmark_text,
        step_name="Run benchmarks",
    )
    assert n == 0


def test_job_filter_applies(fake_client):
    store = InMemoryStore()
    n = ingest_workflow_run(
        client=fake_client, store=store,
        owner="unodb-dev", repo="unodb",
        run=fake_client._runs[0],
        default_parser=google_benchmark_text,
        step_name="Run benchmarks",
        job_filter=lambda j: False,
    )
    assert n == 0


def test_webhook_event_dispatch(fake_client):
    store = InMemoryStore()
    payload = {
        "action": "completed",
        "workflow_run": fake_client._runs[0],
        "repository": {"name": "unodb", "owner": {"login": "unodb-dev"}},
    }
    n = handle_workflow_run_event(
        client=fake_client, store=store, payload=payload,
        default_parser=google_benchmark_text,
        step_name="Run benchmarks",
    )
    assert n == 2


def test_webhook_ignores_non_completed(fake_client):
    store = InMemoryStore()
    payload = {"action": "in_progress"}
    assert handle_workflow_run_event(
        client=fake_client, store=store, payload=payload,
        default_parser=google_benchmark_text,
    ) == 0


def test_client_requires_token():
    with pytest.raises(ValueError):
        GitHubClient("")


# ---------------------------------------------------------------------------
# Webhook HTTP path — 202 Accepted, work runs in the background executor.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_webhook_returns_202_and_runs_ingest_in_background(fake_client, monkeypatch):
    """POST to the webhook route returns 202 immediately; the ingest
    runs on `app.background` and shows up in the store shortly after."""
    from httpx import AsyncClient, ASGITransport
    from nyrkiov3.app import build_app
    from benchzoo.parsers import google_benchmark_text
    import nyrkiov3.app as app_mod
    import nyrkiov3.github_ingest as gi

    # Intercept GitHubClient construction inside the webhook handler so
    # the background task uses our fake.
    monkeypatch.setattr(gi, "GitHubClient", lambda token: fake_client)

    app = build_app()
    app.github_token = "fake"
    app.github_default_parser = google_benchmark_text
    app.github_step_name = "Run benchmarks"

    payload = {
        "action": "completed",
        "workflow_run": fake_client._runs[0],
        "repository": {"name": "unodb", "owner": {"login": "unodb-dev"}},
    }

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post(
            "/api/v3/webhooks/github",
            json=payload,
            headers={"x-github-event": "workflow_run"},
        )

    assert r.status_code == 202, r.text
    body = r.json()
    assert body["accepted"] is True
    assert body["run_id"] == fake_client._runs[0]["id"]

    # Drain the executor to ensure the background task finished.
    app.background.shutdown(wait=True)

    runs = list(app.store.collection("test_runs").find({}))
    assert len(runs) == 2


@pytest.mark.asyncio
async def test_webhook_ignores_non_workflow_run_event():
    from httpx import AsyncClient, ASGITransport
    from nyrkiov3.app import build_app
    app = build_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post(
            "/api/v3/webhooks/github",
            json={"zen": "Anything added dilutes everything else."},
            headers={"x-github-event": "ping"},
        )
    assert r.status_code == 200
    assert r.json()["ignored"] is True


@pytest.mark.asyncio
async def test_webhook_503_when_not_configured():
    from httpx import AsyncClient, ASGITransport
    from nyrkiov3.app import build_app
    app = build_app()  # no github_token / parser configured
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post(
            "/api/v3/webhooks/github",
            json={"action": "completed", "workflow_run": {"id": 1}, "repository": {}},
            headers={"x-github-event": "workflow_run"},
        )
    assert r.status_code == 503
