"""FastAPI app construction + route handlers.

Kept separate from :mod:`promanomaly.main` so the HTTP surface is
testable without booting the scheduler, telemetry, or HA components.
Every route delegates back to the supplied :class:`Application` for
state — the routes themselves carry no state of their own.
"""

from __future__ import annotations

import contextlib
import hmac
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse

from .config import Config, ReloadAuthConfig
from .detectors import list_meta
from .inspect import InspectError, inspect_series

if TYPE_CHECKING:
    from .main import Application


def build_app(application: Application) -> FastAPI:
    """Wire the FastAPI app: lifespan + routes for one Application.

    Routes are closures over ``application`` so reload-driven config
    swaps in ``application._config`` propagate without rebuilding the
    app.
    """

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> Any:
        await application.startup()
        try:
            yield
        finally:
            await application.shutdown()

    app = FastAPI(lifespan=lifespan, title="promanomaly")

    @app.get("/healthz", response_class=PlainTextResponse)
    async def healthz() -> str:
        return "ok"

    @app.get("/ready", response_class=PlainTextResponse)
    async def ready() -> Response:
        if not application._config.server.ready_endpoint:
            return PlainTextResponse("ready endpoint disabled", status_code=404)
        if application._store.any_group_ready():
            return PlainTextResponse("ready", status_code=200)
        return PlainTextResponse("not ready", status_code=503)

    @app.get("/metrics")
    async def metrics() -> Response:
        body, content_type = application._exporter.render()
        return Response(content=body, media_type=content_type)

    @app.post("/-/reload")
    async def reload_endpoint(request: Request) -> Response:
        if not application._config.server.reload.enabled:
            raise HTTPException(status_code=404, detail="reload disabled")
        authorise_reload(application._config.server.reload.auth, request)
        ok, detail = await application.reload()
        if not ok:
            return JSONResponse({"status": "error", "detail": detail}, status_code=400)
        return JSONResponse({"status": "ok"})

    @app.get("/debug/config")
    async def debug_config() -> JSONResponse:
        # Bounded, read-only view of the active config (no secrets).
        return JSONResponse(safe_config_view(application._config))

    @app.get("/debug/detectors")
    async def debug_detectors() -> JSONResponse:
        # Snapshot of the detector entry-point registry. Stable shape so
        # ``detectors list --target`` can render it without negotiation;
        # ``defaults`` is the simple ``{name: default}`` dict kept for
        # backwards compatibility, ``params`` is the canonical richer
        # form with per-parameter type and doc.
        return JSONResponse([m.as_dict() for m in list_meta()])

    @app.get("/debug/inspect")
    async def debug_inspect(
        id: str,
        labels: str | None = None,
    ) -> JSONResponse:
        # Live introspection for one (id, label-selector) series. The
        # NetworkPolicy shipped with the Helm chart denies ingress to
        # /debug/* by default so this is only reachable inside the
        # namespace.
        try:
            payload = await inspect_series(
                config=application._config,
                source=application._source,
                store=application._store,
                metric_id=id,
                label_selector=labels,
            )
        except InspectError as exc:
            client_errors = {"bad_selector", "unknown_id", "discovery_templated"}
            return JSONResponse(
                {"status": "error", "reason": exc.reason, "detail": exc.message},
                status_code=400 if exc.reason in client_errors else 500,
            )
        return JSONResponse(payload)

    return app


def authorise_reload(auth: ReloadAuthConfig, request: Request) -> None:
    """Reject the request unless it satisfies the configured reload auth.

    Bearer is constant-time compared so the response time doesn't leak
    the token length to a timing side-channel. mTLS is enforced by the
    ingress / NetworkPolicy in practice; reaching the handler at all
    implies the TLS handshake succeeded upstream.
    """
    if auth.type == "none":
        return
    if auth.type == "bearer":
        if not auth.token:
            raise HTTPException(status_code=401, detail="unauthorised")
        header = request.headers.get("authorization", "")
        expected = f"Bearer {auth.token}"
        if not hmac.compare_digest(header.encode("utf-8"), expected.encode("utf-8")):
            raise HTTPException(status_code=401, detail="unauthorised")


_SENSITIVE_KEYS: tuple[str, ...] = ("token", "password")


def safe_config_view(config: Config) -> dict[str, Any]:
    """Dump the config with sensitive fields redacted."""
    view = config.model_dump(mode="json")
    for sensitive in _SENSITIVE_KEYS:
        _scrub_keys(view, sensitive)
    return view


def _scrub_keys(obj: Any, key: str) -> None:
    if isinstance(obj, dict):
        for k in list(obj):
            if k == key and obj[k] not in (None, ""):
                obj[k] = "<redacted>"
            else:
                _scrub_keys(obj[k], key)
    elif isinstance(obj, list):
        for item in obj:
            _scrub_keys(item, key)


__all__ = ["authorise_reload", "build_app", "safe_config_view"]
