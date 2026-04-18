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


def _bz_run(test_name, value, *, commit_time=None, ref="main",
            sha="abc123", runner=None, workflow=None, params=None,
            metric_name="latency", unit="ms", passed=True):
    """Construct a benchzoo-shaped test_run the ingest endpoint accepts.

    Matches the shape every benchzoo parser emits: nested ``test`` /
    ``run`` / ``metrics`` / ``commit``. No top-level ``branch`` /
    ``git_commit`` / ``timestamp`` / ``attributes`` — those are the
    old flat fields that no longer exist.
    """
    if commit_time is None:
        commit_time = int(
            datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc).timestamp()
        )
    elif isinstance(commit_time, datetime.datetime):
        commit_time = int(commit_time.timestamp())
    test_block = {"test_name": test_name}
    if params:
        test_block["params"] = params
    run_block = {"passed": passed}
    if runner:
        run_block["runner"] = runner
    if workflow:
        run_block["workflow"] = workflow
    return {
        "test": test_block,
        "run": run_block,
        "env": {"framework": {"name": "test"}},
        "metrics": [{"name": metric_name, "unit": unit, "value": value}],
        "commit": {
            "sha": sha,
            "short_sha": sha[:7],
            "ref": ref,
            "commit_time": commit_time,
        },
    }


async def test_ingest_inserts_and_creates_repo(client, app):
    payload = {"runs": [_bz_run("tpch_q1", 42.1)]}
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
    d1 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    d2 = datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc)
    payload = {"runs": [
        _bz_run("tpch_q1", 42.1, commit_time=d1, sha="s1"),
        _bz_run("tpch_q1", 43.7, commit_time=d2, sha="s2"),
        _bz_run("tpch_q2", 17.0, commit_time=d1, sha="s3"),
    ]}
    r = await client.post("/api/v3/ingest/gh/demo/bench", json=payload)
    assert r.status_code == 200

    r = await client.get("/api/v3/tests/gh/demo/bench")
    assert r.status_code == 200
    results = r.json()
    assert len(results) == 3


async def test_list_filters_by_test_name(client):
    payload = {"runs": [
        _bz_run("tpch_q1", 42.1, sha="s1"),
        _bz_run("tpch_q2", 17.0, sha="s2"),
    ]}
    await client.post("/api/v3/ingest/gh/demo/bench", json=payload)
    r = await client.get("/api/v3/tests/gh/demo/bench?test_name=tpch_q2")
    results = r.json()
    assert len(results) == 1
    assert results[0]["test"]["test_name"] == "tpch_q2"


async def test_list_time_range(client):
    payload = {"runs": [
        _bz_run("t", 1, commit_time=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc), sha="s1"),
        _bz_run("t", 2, commit_time=datetime.datetime(2026, 2, 1, tzinfo=datetime.timezone.utc), sha="s2"),
        _bz_run("t", 3, commit_time=datetime.datetime(2026, 3, 1, tzinfo=datetime.timezone.utc), sha="s3"),
    ]}
    await client.post("/api/v3/ingest/gh/demo/bench", json=payload)
    r = await client.get("/api/v3/tests/gh/demo/bench?since=2026-01-15T00:00:00&until=2026-02-15T00:00:00")
    results = r.json()
    assert len(results) == 1
    assert results[0]["metrics"][0]["value"] == 2


async def test_list_narrows_metric(client):
    run = _bz_run("t", 1)
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


async def test_list_narrows_metric_multi(client):
    """Multi-valued ``?metric=`` selects a subset of each run's metrics."""
    run = _bz_run("t", 1)
    run["metrics"] = [
        {"name": "latency", "unit": "ms", "value": 10},
        {"name": "throughput", "unit": "ops/s", "value": 500},
        {"name": "errors", "unit": "count", "value": 3},
    ]
    await client.post("/api/v3/ingest/gh/demo/bench", json={"runs": [run]})
    r = await client.get("/api/v3/tests/gh/demo/bench?metric=latency&metric=errors")
    results = r.json()
    assert len(results) == 1
    names = {m["name"] for m in results[0]["metrics"]}
    assert names == {"latency", "errors"}


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
    # No "metrics" — required by IngestRun schema.
    bad = {"runs": [{"test": {"test_name": "x"}}]}
    r = await client.post("/api/v3/ingest/gh/demo/bench", json=bad)
    assert r.status_code == 400


async def test_ingest_schema_validation_rejects_run_without_test(client):
    # No "test" — required by IngestRun schema.
    bad = {"runs": [{"metrics": [{"name": "m", "value": 1}]}]}
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


async def test_benchzoo_shaped_end_to_end(client):
    """Five rows on consecutive days; verify ordering on ``commit.commit_time``."""
    runs = [
        _bz_run("sleep_bench", 2.15 + day * 0.01,
                commit_time=datetime.datetime(2026, 1, day, tzinfo=datetime.timezone.utc),
                sha=f"sha_{day}", unit="s", metric_name="wall_time")
        for day in range(1, 6)
    ]
    r = await client.post("/api/v3/ingest/gh/turso/turso", json={"runs": runs})
    assert r.status_code == 200
    assert r.json()["inserted"] == 5

    r = await client.get("/api/v3/tests/gh/turso/turso?test_name=sleep_bench&metric=wall_time")
    data = r.json()
    assert len(data) == 5
    # Sorted by commit time ascending → values come out monotonically increasing.
    values = [d["metrics"][0]["value"] for d in data]
    assert values == sorted(values)


async def test_facets_returns_varying_and_timestamp_span(client):
    # Three runs on two runners; branch constant, runner varies.
    for runner, value in [("intel", 1.0), ("intel", 1.1), ("arm", 2.0)]:
        r = await client.post("/api/v3/ingest/gh/foo/bar", json={"runs": [
            _bz_run("t1", value, runner=runner, workflow="bench.yml",
                    sha=f"sha-{runner}-{value}",
                    commit_time=datetime.datetime(2026, 1, int(value*10) % 9 + 1,
                                                   12, 0, 0,
                                                   tzinfo=datetime.timezone.utc))
        ]})
        assert r.status_code == 200

    r = await client.get("/api/v3/tests/gh/foo/bar/facets")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 3
    assert body["facets"]["branch"] == ["main"]
    assert "branch" not in body["varying"]
    assert set(body["facets"]["runner"]) == {"intel", "arm"}
    assert "runner" in body["varying"]
    assert "timestamp_span" in body and body["timestamp_span"] is not None


async def test_facets_narrow_when_filter_applied(client):
    for runner in ("intel", "arm"):
        await client.post("/api/v3/ingest/gh/foo/bar", json={"runs": [
            _bz_run("t", 1, runner=runner, sha=f"s-{runner}")
        ]})
    # With runner=intel filter, only that value remains.
    r = await client.get("/api/v3/tests/gh/foo/bar/facets?runner=intel")
    body = r.json()
    assert body["facets"]["runner"] == ["intel"]
    assert "runner" not in body["varying"]


async def test_list_filters_on_runner(client):
    for runner in ("intel", "arm"):
        await client.post("/api/v3/ingest/gh/foo/bar", json={"runs": [
            _bz_run("t", 1, runner=runner, sha=f"s-{runner}")
        ]})
    r = await client.get("/api/v3/tests/gh/foo/bar?runner=arm")
    body = r.json()
    assert len(body) == 1
    assert body[0]["run"]["runner"] == "arm"


async def test_facets_surfaces_test_params(client):
    """``test.params`` keys show up as facets — this is how gbench
    ``/N`` args / thread counts become filter dimensions."""
    for size in (64, 512, 4096):
        await client.post("/api/v3/ingest/gh/foo/bar", json={"runs": [
            _bz_run("scan", 1.0, params={"args": str(size)},
                    sha=f"s-{size}")
        ]})
    r = await client.get("/api/v3/tests/gh/foo/bar/facets")
    body = r.json()
    assert "args" in body["facets"]
    assert set(body["facets"]["args"]) == {"64", "512", "4096"}
    assert "args" in body["varying"]


async def test_list_filters_on_test_params(client):
    for size in (64, 512, 4096):
        await client.post("/api/v3/ingest/gh/foo/bar", json={"runs": [
            _bz_run("scan", 1.0, params={"args": str(size)},
                    sha=f"s-{size}")
        ]})
    # Multi-select via repeated params — real-world case for gbench arg ranges.
    r = await client.get("/api/v3/tests/gh/foo/bar?args=64&args=4096")
    body = r.json()
    assert len(body) == 2
    assert {d["test"]["params"]["args"] for d in body} == {"64", "4096"}


async def test_filter_value_containing_commas(client):
    """Runner names from GitHub Actions legitimately contain commas
    (``"benchmark (Linux ARM64, ubuntu-24.04-arm, arm64)"``). Repeated
    query params must preserve the comma as content, not a separator."""
    nasty = "benchmark (Linux ARM64, ubuntu-24.04-arm, arm64)"
    clean = "benchmark (Linux Intel, nyrkio_8, x64)"
    for r in (nasty, clean):
        await client.post("/api/v3/ingest/gh/foo/bar", json={"runs": [
            _bz_run("t", 1, runner=r, sha=f"s-{r[:4]}")
        ]})
    resp = await client.get("/api/v3/tests/gh/foo/bar",
                            params=[("runner", nasty)])
    body = resp.json()
    assert len(body) == 1
    assert body[0]["run"]["runner"] == nasty
