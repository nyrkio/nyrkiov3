"""GitHub OAuth + session cookies.

Session format is a signed, URL-safe string:

    <base64-json-payload> "." <base64-hmac-sha256>

No external deps — the library only uses stdlib. For production, the
session secret should be a 32-byte random value held in env vars and
rotated periodically; clients with an old-secret-signed cookie simply
re-log in (we don't support dual-secret rollover here).

Access tokens are stored plaintext in the users collection for the
initial cut; wrapping them with AES in a cold key is a TODO.
"""
from __future__ import annotations

import base64
import hmac
import hashlib
import json
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request


AUTHORIZE_URL = "https://github.com/login/oauth/authorize"
TOKEN_URL = "https://github.com/login/oauth/access_token"
API_USER_URL = "https://api.github.com/user"


class AuthError(Exception):
    """Raised when a session cookie is invalid / expired / untrusted."""


# ---------- session cookie codec ------------------------------------------

def _b64e(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64d(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def sign_session(payload: dict, secret: str, ttl_s: int = 30 * 86400) -> str:
    """Return a signed cookie string from the given payload dict."""
    body = dict(payload)
    body.setdefault("exp", int(time.time()) + ttl_s)
    raw = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    b = _b64e(raw)
    sig = hmac.new(secret.encode("utf-8"), b.encode("ascii"), hashlib.sha256).digest()
    return f"{b}.{_b64e(sig)}"


def verify_session(cookie: str, secret: str) -> dict:
    """Return the payload dict; raises AuthError if invalid / expired."""
    if not cookie or "." not in cookie:
        raise AuthError("malformed session cookie")
    b, s = cookie.split(".", 1)
    expected = hmac.new(secret.encode("utf-8"), b.encode("ascii"), hashlib.sha256).digest()
    actual = _b64d(s)
    if not hmac.compare_digest(expected, actual):
        raise AuthError("bad session signature")
    try:
        payload = json.loads(_b64d(b))
    except Exception as e:
        raise AuthError(f"payload decode failed: {e}")
    if payload.get("exp", 0) < int(time.time()):
        raise AuthError("session expired")
    return payload


# ---------- OAuth helpers -------------------------------------------------

def new_state() -> str:
    """Random CSRF state for the authorize → callback handoff."""
    return secrets.token_urlsafe(24)


def authorize_url(client_id: str, redirect_uri: str, state: str,
                  scope: str = "repo read:user") -> str:
    qs = urllib.parse.urlencode({
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "state": state,
        "scope": scope,
        "allow_signup": "true",
    })
    return f"{AUTHORIZE_URL}?{qs}"


def exchange_code(client_id: str, client_secret: str, code: str,
                  redirect_uri: str) -> str:
    """POST the code to GitHub; return an access_token string."""
    body = urllib.parse.urlencode({
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "redirect_uri": redirect_uri,
    }).encode("utf-8")
    req = urllib.request.Request(
        TOKEN_URL, data=body, method="POST",
        headers={"Accept": "application/json",
                 "User-Agent": "nyrkio-oauth/0.1"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if "access_token" not in data:
        raise AuthError(f"GitHub did not return an access_token: {data!r}")
    return data["access_token"]


def fetch_user(access_token: str) -> dict:
    req = urllib.request.Request(
        API_USER_URL,
        headers={"Authorization": f"Bearer {access_token}",
                 "Accept": "application/vnd.github+json",
                 "User-Agent": "nyrkio-oauth/0.1"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))
