import datetime
import pytest
from httpx import AsyncClient, ASGITransport
from nyrkiov3.app import build_app


pytestmark = pytest.mark.asyncio


@pytest.fixture
def app():
    return build_app()


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _benchzoo_run(test_name, value, ts=None, branch="main", commit="abc123"):
    """Construct a benchzoo-shaped test_run payload."""
    return {
        "branch": branch,
        "git_commit": commit,
        "timestamp": (ts or datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)).isoformat(),
        "attributes": {"test_name": test_name},
        "metrics": [{"name": "latency", "unit": "ms", "value": value}],
        "passed": True,
    }


async def test_ingest_inserts_and_creates_repo(client, app):
    payload = {"runs": [_benchzoo_run("tpch_q1", 42.1)]}
    r = await client.post("/api/v3/ingest/gh/demo/bench", json=payload)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["inserted"] == 1

    # Repo was created.
    repos = app.store.collection("repos")
    assert repos.count() == 1
    repo = repos.find_one({"absolute_name": "gh/demo/bench"})
    assert repo["namespace"] == "demo"


async def test_ingest_then_list(client):
    payload = {
        "runs": [
            _benchzoo_run("tpch_q1", 42.1, datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)),
            _benchzoo_run("tpch_q1", 43.7, datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc)),
            _benchzoo_run("tpch_q2", 17.0, datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)),
        ]
    }
    r = await client.post("/api/v3/ingest/gh/demo/bench", json=payload)
    assert r.status_code == 200

    r = await client.get("/api/v3/tests/gh/demo/bench")
    assert r.status_code == 200
    results = r.json()
    assert len(results) == 3


async def test_list_filters_by_test_name(client):
    payload = {
        "runs": [
            _benchzoo_run("tpch_q1", 42.1),
            _benchzoo_run("tpch_q2", 17.0),
        ]
    }
    await client.post("/api/v3/ingest/gh/demo/bench", json=payload)
    r = await client.get("/api/v3/tests/gh/demo/bench?test_name=tpch_q2")
    results = r.json()
    assert len(results) == 1
    assert results[0]["attributes"]["test_name"] == "tpch_q2"


async def test_list_time_range(client):
    payload = {
        "runs": [
            _benchzoo_run("t", 1, datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)),
            _benchzoo_run("t", 2, datetime.datetime(2026, 2, 1, tzinfo=datetime.timezone.utc)),
            _benchzoo_run("t", 3, datetime.datetime(2026, 3, 1, tzinfo=datetime.timezone.utc)),
        ]
    }
    await client.post("/api/v3/ingest/gh/demo/bench", json=payload)
    r = await client.get("/api/v3/tests/gh/demo/bench?since=2026-01-15T00:00:00&until=2026-02-15T00:00:00")
    results = r.json()
    assert len(results) == 1
    assert results[0]["metrics"][0]["value"] == 2


async def test_list_narrows_metric(client):
    run = _benchzoo_run("t", 1)
    run["metrics"] = [
        {"name": "latency", "unit": "ms", "value": 10},
        {"name": "throughput", "unit": "ops/s", "value": 500},
    ]
    await client.post("/api/v3/ingest/gh/demo/bench", json={"runs": [run]})
    r = await client.get("/api/v3/tests/gh/demo/bench?metric=throughput")
    results = r.json()
    assert len(results) == 1
    assert len(results[0]["metrics"]) == 1
    assert results[0]["metrics"][0]["name"] == "throughput"


async def test_unknown_route_404(client):
    r = await client.get("/api/v3/nonsense")
    assert r.status_code == 404
    assert "no route" in r.json()["error"]


async def test_ingest_schema_validation_rejects_bad_payload(client):
    # Missing "runs" at top level.
    r = await client.post("/api/v3/ingest/gh/demo/bench", json={"nope": []})
    assert r.status_code == 400
    assert "validation" in r.json()["error"]


async def test_ingest_schema_validation_rejects_run_without_metrics(client):
    bad = {"runs": [{"attributes": {"test_name": "x"}}]}  # no "metrics" array
    r = await client.post("/api/v3/ingest/gh/demo/bench", json=bad)
    assert r.status_code == 400


async def test_middleware_event_fires():
    app = build_app()
    seen = []

    @app.on("before_handler")
    def tag(request):
        seen.append(request["path"])

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        await c.get("/api/v3/tests/gh/demo/bench")
    assert seen == ["/api/v3/tests/gh/demo/bench"]


async def test_ingest_rejects_naive_iso_timestamp(client):
    """Naive ISO strings should fail loudly — no silent UTC promotion."""
    bad = {
        "runs": [{
            "attributes": {"test_name": "x"},
            "timestamp": "2026-01-01T12:00:00",  # no Z, no offset
            "metrics": [{"name": "m", "value": 1.0}],
        }]
    }
    r = await client.post("/api/v3/ingest/gh/demo/bench", json=bad)
    assert r.status_code == 500
    assert "naive datetime" in r.json()["error"].lower() or "tzinfo" in r.json()["error"].lower() or "timezone" in r.json()["error"].lower()


async def test_benchzoo_shaped_end_to_end(client):
    """End-to-end exercise matching what benchzoo parsers actually emit."""
    runs = []
    for day in range(1, 6):
        runs.append({
            "attributes": {"test_name": "sleep_bench"},
            "timestamp": datetime.datetime(2026, 1, day, tzinfo=datetime.timezone.utc).isoformat(),
            "metrics": [{"name": "wall_time", "unit": "s", "value": 2.15 + day * 0.01}],
            "branch": "main",
            "git_commit": f"commit_{day}",
            "passed": True,
        })
    r = await client.post("/api/v3/ingest/gh/turso/turso", json={"runs": runs})
    assert r.status_code == 200
    assert r.json()["inserted"] == 5

    r = await client.get("/api/v3/tests/gh/turso/turso?test_name=sleep_bench&metric=wall_time")
    data = r.json()
    assert len(data) == 5
    # Sorted by timestamp ascending.
    values = [d["metrics"][0]["value"] for d in data]
    assert values == sorted(values)
