"""Tests for session cookie signing + auth-required routes."""
from __future__ import annotations

import time

import pytest

from nyrkiov3 import auth as _auth


def test_session_roundtrip():
    cookie = _auth.sign_session({"github_id": 42, "username": "me"}, "secret")
    payload = _auth.verify_session(cookie, "secret")
    assert payload["github_id"] == 42
    assert payload["username"] == "me"
    assert payload["exp"] > int(time.time())


def test_session_rejects_bad_signature():
    cookie = _auth.sign_session({"github_id": 1}, "secret-a")
    with pytest.raises(_auth.AuthError):
        _auth.verify_session(cookie, "secret-b")


def test_session_rejects_expired():
    # Manually craft a payload with exp in the past.
    import json, base64, hmac, hashlib
    body = json.dumps({"exp": 1}, separators=(",", ":"), sort_keys=True).encode()
    b = base64.urlsafe_b64encode(body).rstrip(b"=").decode()
    sig = base64.urlsafe_b64encode(
        hmac.new(b"s", b.encode(), hashlib.sha256).digest()).rstrip(b"=").decode()
    with pytest.raises(_auth.AuthError):
        _auth.verify_session(f"{b}.{sig}", "s")


def test_session_rejects_malformed():
    with pytest.raises(_auth.AuthError):
        _auth.verify_session("nodot", "s")
    with pytest.raises(_auth.AuthError):
        _auth.verify_session("", "s")


@pytest.mark.asyncio
async def test_me_returns_401_without_cookie():
    from httpx import AsyncClient, ASGITransport
    from nyrkiov3.app import build_app
    app = build_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/api/v3/me")
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_login_503_when_oauth_not_configured():
    from httpx import AsyncClient, ASGITransport
    from nyrkiov3.app import build_app
    app = build_app()  # no env vars set in tests
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/login")
    # No redirect, no oauth → 503 politely.
    assert r.status_code == 503


@pytest.mark.asyncio
async def test_public_connect_requires_app_token():
    from httpx import AsyncClient, ASGITransport
    from nyrkiov3.app import build_app
    import os
    # Explicitly clear tokens.
    for k in ("CLAUDE_GITHUB_PAT",):
        os.environ.pop(k, None)
    app = build_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.post("/api/v3/public/connect", json={"repo": "owner/name"})
    assert r.status_code == 503


@pytest.mark.asyncio
async def test_public_connect_validates_repo_shape():
    from httpx import AsyncClient, ASGITransport
    from nyrkiov3.app import build_app
    import os
    os.environ["CLAUDE_GITHUB_PAT"] = "fake"
    try:
        app = build_app()
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.post("/api/v3/public/connect", json={"repo": "bad-no-slash"})
        assert r.status_code == 400
    finally:
        os.environ.pop("CLAUDE_GITHUB_PAT", None)
