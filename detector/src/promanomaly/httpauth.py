"""Shared httpx client kwargs for the :class:`~promanomaly.config.AuthConfig`
auth shapes (bearer / basic / mTLS).

Used by the remote-write sink and the metrics adapter so the four auth
shapes are wired identically wherever promanomaly talks to an
authenticated HTTP endpoint.
"""

from __future__ import annotations

from typing import Any

import httpx

from .config import AuthConfig


def build_httpx_auth(auth: AuthConfig) -> dict[str, Any]:
    """Return httpx ``AsyncClient`` kwargs implementing ``auth``.

    Returns an empty dict for ``type: none``. Bearer sets the
    Authorization header; basic sets httpx BasicAuth; mTLS sets the client
    cert pair and (optionally) the CA bundle to verify against. Bearer
    tokens and basic passwords are resolved from their ``*_file`` Secret
    mount when configured (the path the Helm charts wire up), else the
    inline value.
    """
    kwargs: dict[str, Any] = {}
    if auth.type == "bearer":
        token = auth.resolved_token()
        if token:
            kwargs["headers"] = {"Authorization": f"Bearer {token}"}
    elif auth.type == "basic" and auth.username:
        kwargs["auth"] = httpx.BasicAuth(auth.username, auth.resolved_password() or "")
    elif auth.type == "mtls":
        if auth.cert_file and auth.key_file:
            kwargs["cert"] = (auth.cert_file, auth.key_file)
        if auth.ca_file:
            kwargs["verify"] = auth.ca_file
    return kwargs


__all__ = ["build_httpx_auth"]
