"""Shared httpx client kwargs for the :class:`~promanomaly.config.AuthConfig`
auth shapes (bearer / basic / mTLS / sigv4 / gcp / azure / oauth2).

Used by the remote-write sink and the metrics adapter so the auth
shapes are wired identically wherever promanomaly talks to an
authenticated HTTP endpoint.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import httpx

from .config import AuthConfig


class _OAuth2TokenCache:
    """Thread-safe cache for an OAuth2 access token with auto-refresh.

    Performs a ``client_credentials`` grant against ``token_url`` and
    caches the resulting token. Refreshes transparently when the token
    is within 60 seconds of expiry.
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scopes: list[str],
    ) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scopes = scopes
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at: float = 0.0

    def get_token(self) -> str:
        """Return a valid access token, refreshing if needed."""
        with self._lock:
            if self._token and time.monotonic() < self._expires_at - 60:
                return self._token
            self._refresh()
            assert self._token is not None
            return self._token

    def _refresh(self) -> None:
        data: dict[str, str] = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        if self._scopes:
            data["scope"] = " ".join(self._scopes)
        try:
            resp = httpx.post(self._token_url, data=data, timeout=10.0)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise RuntimeError(
                f"OAuth2 token refresh failed against {self._token_url}: {exc}"
            ) from exc
        try:
            payload = resp.json()
        except ValueError as exc:
            raise RuntimeError(
                f"OAuth2 token endpoint returned non-JSON response: {resp.text[:200]}"
            ) from exc
        if "access_token" not in payload:
            raise RuntimeError(
                f"OAuth2 token response missing 'access_token' key: {sorted(payload.keys())}"
            )
        self._token = str(payload["access_token"])
        expires_in = float(payload.get("expires_in", 3600))
        self._expires_at = time.monotonic() + expires_in


class _CloudAuthEventHook:
    """Async httpx event hook that injects cloud-provider auth headers.

    httpx ``AsyncClient`` awaits request hooks, so this hook is async.
    Token refresh and SigV4 signing involve blocking I/O (HTTP calls to
    token endpoints, credential chain resolution); these are offloaded
    to a worker thread via ``asyncio.to_thread`` so the event loop is
    never blocked.
    """

    def __init__(self, auth: AuthConfig) -> None:
        self._auth = auth
        self._type = auth.type
        # Lazily initialised on first use so missing SDKs surface an
        # actionable error rather than a boot-time ImportError.
        self._initialised = False
        self._lock = threading.Lock()
        self._oauth2_cache: _OAuth2TokenCache | None = None
        self._gcp_credentials: Any = None
        self._azure_credential: Any = None
        self._sigv4_session: Any = None
        self._sigv4_service: str = "aps"

    def _ensure_initialised(self) -> None:
        if self._initialised:
            return
        with self._lock:
            if self._initialised:
                return
            self._do_init()
            self._initialised = True

    def _do_init(self) -> None:
        if self._type == "oauth2":
            assert self._auth.oauth2 is not None
            secret = self._auth.oauth2.resolved_client_secret() or ""
            self._oauth2_cache = _OAuth2TokenCache(
                token_url=self._auth.oauth2.token_url,
                client_id=self._auth.oauth2.client_id,
                client_secret=secret,
                scopes=list(self._auth.oauth2.scopes),
            )
        elif self._type == "gcp":
            try:
                import google.auth
                import google.auth.transport.requests
            except ImportError as exc:
                raise ImportError(
                    "auth.type=gcp requires the [gcp] extra: pip install promanomaly[gcp]"
                ) from exc
            gcp_cfg = self._auth.gcp
            if gcp_cfg and gcp_cfg.credentials_file:
                self._gcp_credentials, _ = google.auth.load_credentials_from_file(  # type: ignore[no-untyped-call, unused-ignore]
                    gcp_cfg.credentials_file,
                    scopes=["https://www.googleapis.com/auth/monitoring.read"],
                )
            else:
                self._gcp_credentials, _ = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/monitoring.read"],
                )
        elif self._type == "azure":
            try:
                from azure.identity import (
                    ClientSecretCredential,
                    DefaultAzureCredential,
                )
            except ImportError as exc:
                raise ImportError(
                    "auth.type=azure requires the [azure] extra: pip install promanomaly[azure]"
                ) from exc
            az_cfg = self._auth.azure
            if az_cfg and az_cfg.client_id and az_cfg.tenant_id and az_cfg.client_secret_file:
                from .config import _read_secret_file

                secret = _read_secret_file(az_cfg.client_secret_file)
                self._azure_credential = ClientSecretCredential(
                    tenant_id=az_cfg.tenant_id,
                    client_id=az_cfg.client_id,
                    client_secret=secret,
                )
            else:
                self._azure_credential = DefaultAzureCredential()
        elif self._type == "sigv4":
            try:
                import botocore.session
            except ImportError as exc:
                raise ImportError(
                    "auth.type=sigv4 requires the [aws] extra: pip install promanomaly[aws]"
                ) from exc
            assert self._auth.sigv4 is not None
            self._sigv4_session = botocore.session.get_session()
            if self._auth.sigv4.profile:
                self._sigv4_session.set_config_variable("profile", self._auth.sigv4.profile)

    def _inject_auth_sync(self, request: httpx.Request) -> None:
        """Synchronous auth injection — runs in a worker thread."""
        self._ensure_initialised()

        if self._type == "oauth2":
            assert self._oauth2_cache is not None
            token = self._oauth2_cache.get_token()
            request.headers["Authorization"] = f"Bearer {token}"
        elif self._type == "gcp":
            import google.auth.transport.requests

            if not self._gcp_credentials.valid:
                self._gcp_credentials.refresh(google.auth.transport.requests.Request())
            request.headers["Authorization"] = f"Bearer {self._gcp_credentials.token}"
        elif self._type == "azure":
            token = self._azure_credential.get_token(
                "https://prometheus.monitor.azure.com/.default"
            )
            request.headers["Authorization"] = f"Bearer {token.token}"
        elif self._type == "sigv4":
            self._sign_sigv4(request)

    async def inject_auth(self, request: httpx.Request) -> None:
        """Async event hook for httpx ``AsyncClient``.

        Offloads the blocking auth work (token refresh, credential
        resolution, SigV4 signing) to a worker thread so the event
        loop is never blocked.
        """
        await asyncio.to_thread(self._inject_auth_sync, request)

    def _sign_sigv4(self, request: httpx.Request) -> None:
        """Sign a request with AWS SigV4 for Amazon Managed Prometheus."""
        import botocore.auth
        from botocore.awsrequest import AWSRequest

        assert self._auth.sigv4 is not None
        credentials = self._sigv4_session.get_credentials()
        if credentials is None:
            raise RuntimeError("no AWS credentials found for SigV4 signing")
        resolved = credentials.get_frozen_credentials()
        signer = botocore.auth.SigV4Auth(
            resolved,
            self._sigv4_service,
            self._auth.sigv4.region,
        )
        aws_request = AWSRequest(
            method=str(request.method),
            url=str(request.url),
            headers=dict(request.headers),
            data=request.content,
        )
        signer.add_auth(aws_request)
        # Copy the signed headers back.
        for key, value in aws_request.headers.items():
            request.headers[key] = value


def build_httpx_auth(auth: AuthConfig) -> dict[str, Any]:
    """Return httpx ``AsyncClient`` kwargs implementing ``auth``.

    Returns an empty dict for ``type: none``. Bearer sets the
    Authorization header; basic sets httpx BasicAuth; mTLS sets the client
    cert pair and (optionally) the CA bundle to verify against. Cloud auth
    types (sigv4, gcp, azure, oauth2) install an async event hook that
    offloads blocking token refresh to a worker thread so the event loop
    is never blocked. Bearer tokens and basic passwords are resolved from
    their ``*_file`` Secret mount when configured (the path the Helm
    charts wire up), else the inline value.
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
    elif auth.type in ("sigv4", "gcp", "azure", "oauth2"):
        hook = _CloudAuthEventHook(auth)
        kwargs["event_hooks"] = {"request": [hook.inject_auth]}
    return kwargs


__all__ = ["build_httpx_auth"]
