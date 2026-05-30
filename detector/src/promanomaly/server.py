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

from .anomalies import collect_blast_radius, collect_firing
from .config import Config, ReloadAuthConfig
from .detectors import list_meta
from .inspect import InspectError, inspect_series
from .metadata_lint import lint_config_metadata
from .source import SourceQueryError
from .warmup import compute_warmup

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

    @app.get("/debug/anomalies")
    async def debug_anomalies(
        group: str | None = None,
        min_severity: float = 0.0,
        limit: int = 50,
        lint: bool = False,
    ) -> JSONResponse:
        # Live triage: the currently-firing series across all groups,
        # ranked by severity, read straight from the snapshot store (no
        # extra TSDB queries). Pairs with /debug/inspect — top to find
        # the worst series, inspect to understand why it scored. The
        # NetworkPolicy denies ingress to /debug/* by default.
        firing = collect_firing(
            application._store,
            group=group,
            min_severity=min_severity,
            limit=max(0, limit),
        )
        blast = collect_blast_radius(application._store)
        if group is not None:
            blast = [r for r in blast if r.group == group]
        body: dict[str, Any] = {
            "count": len(firing),
            "min_severity": min_severity,
            "group": group,
            "anomalies": [f.as_dict() for f in firing],
            "blast_radius": [r.as_dict() for r in blast],
        }
        # Opt-in metadata lint (off by default so the no-extra-TSDB-query
        # contract holds for the common scrape path). ``promanomaly top``
        # requests it so an operator triaging an anomaly is reminded when
        # the offending query is feeding a raw counter to a detector.
        if lint:
            body["metadata_lint"] = await _metadata_lint_hints(application)
        return JSONResponse(body)

    @app.get("/warmup")
    async def warmup_endpoint() -> Response:
        # Opt-in per server.expose_warmup_endpoint. Reports per-(group,
        # query) warm-up status by issuing one cheap probe per query —
        # the operator's answer to "the dashboard is empty, do I wait or
        # debug?" on a fresh install. The shipped NetworkPolicy denies
        # external ingress here alongside /debug/*.
        if not application._config.server.expose_warmup_endpoint:
            return PlainTextResponse("warmup endpoint disabled", status_code=404)
        try:
            report = await compute_warmup(
                config=application._config,
                source=application._source,
            )
        except Exception as exc:  # pragma: no cover - defensive
            return JSONResponse(
                {"error": "internal", "message": str(exc)},
                status_code=500,
            )
        return JSONResponse(report.to_dict())

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


async def _metadata_lint_hints(application: Application) -> list[dict[str, Any]]:
    """Best-effort metadata-lint findings for the live config.

    Used by ``/debug/anomalies?lint=true`` (and thus ``promanomaly top
    --lint``). A TSDB without a metadata endpoint, or a transient query
    failure, yields no hints rather than failing triage.
    """
    try:
        metadata = await application._source.metric_metadata()
    except SourceQueryError:
        return []
    except Exception:  # pragma: no cover - defensive
        return []
    return [f.to_dict() for f in lint_config_metadata(application._config, metadata)]


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
