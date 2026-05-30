"""File-based credential resolution shared across datasource, sinks, adapter."""

from __future__ import annotations

from pathlib import Path

from promanomaly.config import AuthConfig
from promanomaly.httpauth import build_httpx_auth


def test_resolved_token_prefers_file(tmp_path: Path) -> None:
    token_path = tmp_path / "token"
    token_path.write_text("sekret-from-file\n")  # trailing newline trimmed
    auth = AuthConfig(type="bearer", token="inline", token_file=str(token_path))
    assert auth.resolved_token() == "sekret-from-file"


def test_resolved_token_falls_back_to_inline() -> None:
    auth = AuthConfig(type="bearer", token="inline")
    assert auth.resolved_token() == "inline"


def test_resolved_password_prefers_file(tmp_path: Path) -> None:
    pw = tmp_path / "password"
    pw.write_text("hunter2\n")
    auth = AuthConfig(type="basic", username="u", password="x", password_file=str(pw))
    assert auth.resolved_password() == "hunter2"


def test_build_httpx_auth_bearer_from_file(tmp_path: Path) -> None:
    token_path = tmp_path / "token"
    token_path.write_text("filetoken")
    auth = AuthConfig(type="bearer", token_file=str(token_path))
    kwargs = build_httpx_auth(auth)
    assert kwargs["headers"]["Authorization"] == "Bearer filetoken"


def test_build_httpx_auth_basic_password_from_file(tmp_path: Path) -> None:
    pw = tmp_path / "password"
    pw.write_text("pw")
    auth = AuthConfig(type="basic", username="user", password_file=str(pw))
    kwargs = build_httpx_auth(auth)
    # httpx BasicAuth carries the credential; verify it encodes user:pw.
    import base64

    basic = kwargs["auth"]
    request = next(basic.auth_flow(_DummyRequest()))
    expected = "Basic " + base64.b64encode(b"user:pw").decode()
    assert request.headers["Authorization"] == expected


def test_build_httpx_auth_none_is_empty() -> None:
    assert build_httpx_auth(AuthConfig(type="none")) == {}


def test_build_httpx_auth_mtls() -> None:
    auth = AuthConfig(type="mtls", cert_file="/c.crt", key_file="/c.key", ca_file="/ca.crt")
    kwargs = build_httpx_auth(auth)
    assert kwargs["cert"] == ("/c.crt", "/c.key")
    assert kwargs["verify"] == "/ca.crt"


class _DummyRequest:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
