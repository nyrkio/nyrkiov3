"""Precedence and discovery tests for the config loader.

The loader's contract: CLI > env > explicit ``-c`` config file >
default ``./nyrkio.yml`` > ``~/.nyrkiov3/...`` > ``/etc/nyrkiov3/...``
> built-in default. Only the first three are easy to exercise from
a test (the system paths require root)."""
from __future__ import annotations

import os
import pathlib

import pytest

from nyrkiov3 import config


# Env vars the loader reads. Tests must not leak these between cases —
# `clean_env` clears them before and after each test.
NYRKIO_ENV_VARS = [
    "NYRKIO_STORAGE_PATH", "NYRKIO_MONGO_URI", "NYRKIO_MONGO_DB",
    "NYRKIO_STATIC_DIR", "NYRKIO_AURORA_DIR", "NYRKIO_BIND",
    "NYRKIO_BASE_URL", "NYRKIO_GITHUB_CLIENT_ID",
    "NYRKIO_GITHUB_CLIENT_SECRET", "NYRKIO_SESSION_SECRET",
    "NYRKIO_APP_GITHUB_PAT", "NYRKIO_SNAPSHOT_INTERVAL",
    "CLAUDE_GITHUB_PAT", "GITHUB_TOKEN",
]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Wipe NYRKIO_* / token env vars so test cases see a known state."""
    for name in NYRKIO_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def isolated_cwd(tmp_path, monkeypatch):
    """Run each test in an empty CWD so ./nyrkio.yml from the repo
    doesn't leak in."""
    monkeypatch.chdir(tmp_path)
    yield tmp_path


@pytest.fixture(autouse=True)
def no_default_files(monkeypatch):
    """Prevent the loader from reading real ``/etc`` or ``~`` files
    on the developer's machine."""
    monkeypatch.setattr(config, "DEFAULT_CONFIG_FILES", [])


# --------- serve loader -----------------------------------------------

def test_serve_defaults():
    cfg = config.load_serve_config([])
    assert cfg["bind"] == "127.0.0.1:8123"
    assert cfg["mongo_db"] == "nyrkio"
    assert cfg["base_url"] == "https://nyrkio.com"
    assert cfg["github_client_id"] == ""
    assert cfg["session_secret"] == ""
    # storage_path expanded:
    assert "~" not in cfg["storage_path"]


def test_cli_beats_default():
    cfg = config.load_serve_config(["--bind", "0.0.0.0:9000"])
    assert cfg["bind"] == "0.0.0.0:9000"


def test_env_beats_default(monkeypatch):
    monkeypatch.setenv("NYRKIO_BIND", "10.0.0.1:8080")
    monkeypatch.setenv("NYRKIO_SESSION_SECRET", "deadbeef" * 4)
    cfg = config.load_serve_config([])
    assert cfg["bind"] == "10.0.0.1:8080"
    assert cfg["session_secret"] == "deadbeef" * 4


def test_cli_beats_env(monkeypatch):
    monkeypatch.setenv("NYRKIO_BIND", "10.0.0.1:8080")
    cfg = config.load_serve_config(["--bind", "127.0.0.1:9999"])
    assert cfg["bind"] == "127.0.0.1:9999"


def test_yaml_config_file(tmp_path):
    cfg_path = tmp_path / "nyrkio.yml"
    cfg_path.write_text(
        "bind: 127.0.0.1:7000\n"
        "base-url: https://example.test/v3\n"
        "github-client-id: abc123\n"
    )
    cfg = config.load_serve_config(["-c", str(cfg_path)])
    assert cfg["bind"] == "127.0.0.1:7000"
    assert cfg["base_url"] == "https://example.test/v3"
    assert cfg["github_client_id"] == "abc123"


def test_env_beats_config_file(tmp_path, monkeypatch):
    cfg_path = tmp_path / "nyrkio.yml"
    cfg_path.write_text("bind: 127.0.0.1:7000\n")
    monkeypatch.setenv("NYRKIO_BIND", "127.0.0.1:9999")
    cfg = config.load_serve_config(["-c", str(cfg_path)])
    assert cfg["bind"] == "127.0.0.1:9999"


def test_cli_beats_config_file(tmp_path):
    cfg_path = tmp_path / "nyrkio.yml"
    cfg_path.write_text("bind: 127.0.0.1:7000\n")
    cfg = config.load_serve_config(["-c", str(cfg_path),
                                    "--bind", "127.0.0.1:9999"])
    assert cfg["bind"] == "127.0.0.1:9999"


def test_secrets_merge_with_config(tmp_path, monkeypatch):
    """The intended config + secrets split: two YAML files in the
    auto-discovery list. Both get merged into one config dict."""
    public = tmp_path / "config.yml"
    public.write_text(
        "bind: 127.0.0.1:7000\n"
        "github-client-id: public-id\n"
    )
    secrets = tmp_path / "secrets.yml"
    secrets.write_text(
        "github-client-secret: shhh\n"
        "session-secret: " + "f" * 64 + "\n"
    )
    monkeypatch.setattr(config, "DEFAULT_CONFIG_FILES",
                        [str(public), str(secrets)])
    cfg = config.load_serve_config([])
    assert cfg["bind"] == "127.0.0.1:7000"
    assert cfg["github_client_id"] == "public-id"
    assert cfg["github_client_secret"] == "shhh"
    assert cfg["session_secret"] == "f" * 64


def test_claude_pat_fallback(monkeypatch):
    monkeypatch.setenv("CLAUDE_GITHUB_PAT", "fallback-token")
    cfg = config.load_serve_config([])
    assert cfg["app_github_pat"] == "fallback-token"


def test_explicit_pat_beats_claude_fallback(monkeypatch):
    monkeypatch.setenv("CLAUDE_GITHUB_PAT", "fallback")
    monkeypatch.setenv("NYRKIO_APP_GITHUB_PAT", "primary")
    cfg = config.load_serve_config([])
    assert cfg["app_github_pat"] == "primary"


def test_default_config_files_discovered(tmp_path, monkeypatch):
    """When a path lives in DEFAULT_CONFIG_FILES, the loader reads it."""
    cfg_path = tmp_path / "auto.yml"
    cfg_path.write_text("bind: 127.0.0.1:6543\n")
    monkeypatch.setattr(config, "DEFAULT_CONFIG_FILES", [str(cfg_path)])
    cfg = config.load_serve_config([])
    assert cfg["bind"] == "127.0.0.1:6543"


def test_later_default_file_overrides_earlier(tmp_path, monkeypatch):
    """Discovery order is low → high priority: later files win."""
    low = tmp_path / "system.yml"
    high = tmp_path / "user.yml"
    low.write_text("bind: 127.0.0.1:1111\nbase-url: https://low.test\n")
    high.write_text("bind: 127.0.0.1:2222\n")
    monkeypatch.setattr(config, "DEFAULT_CONFIG_FILES",
                        [str(low), str(high)])
    cfg = config.load_serve_config([])
    assert cfg["bind"] == "127.0.0.1:2222"  # high wins
    assert cfg["base_url"] == "https://low.test"  # only low set it


# --------- sync loader ------------------------------------------------

def test_sync_requires_repo_workflow_parser():
    with pytest.raises(SystemExit):
        config.load_sync_config([])


def test_sync_minimal(monkeypatch):
    monkeypatch.setenv("CLAUDE_GITHUB_PAT", "tok")
    cfg = config.load_sync_config([
        "--repo", "foo/bar",
        "--workflow", "ci.yml",
        "--parser", "google-benchmark/text",
    ])
    assert cfg["repo"] == "foo/bar"
    assert cfg["workflow"] == "ci.yml"
    assert cfg["parser"] == "google-benchmark/text"
    assert cfg["github_token"] == "tok"
    assert cfg["storage_path"].endswith("secantus")


def test_sync_github_token_fallback_order(monkeypatch):
    """--github-token > NYRKIO_APP_GITHUB_PAT > CLAUDE_GITHUB_PAT > GITHUB_TOKEN."""
    monkeypatch.setenv("GITHUB_TOKEN", "last")
    monkeypatch.setenv("CLAUDE_GITHUB_PAT", "third")
    monkeypatch.setenv("NYRKIO_APP_GITHUB_PAT", "second")
    base = ["--repo", "f/b", "--workflow", "ci.yml",
            "--parser", "google-benchmark/text"]
    cfg = config.load_sync_config(base + ["--github-token", "first"])
    assert cfg["github_token"] == "first"
    cfg = config.load_sync_config(base)
    assert cfg["github_token"] == "second"
    monkeypatch.delenv("NYRKIO_APP_GITHUB_PAT")
    cfg = config.load_sync_config(base)
    assert cfg["github_token"] == "third"
    monkeypatch.delenv("CLAUDE_GITHUB_PAT")
    cfg = config.load_sync_config(base)
    assert cfg["github_token"] == "last"
