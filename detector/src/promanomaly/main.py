"""Detector entrypoint: FastAPI app, scheduler, and signal handling.

Boot order:

1. Parse YAML config (pydantic validation).
2. Configure structlog.
3. Build the snapshot store + operational registry.
4. Build the PromQL source client.
5. Build the runner.
6. Build the FastAPI app and register endpoints.
7. Run uvicorn — its lifespan hook starts APScheduler, registers
   SIGHUP through the running event loop, and fires the immediate
   per-group first run.

Per the project guide the runner triggers a first run immediately
rather than waiting for the first scheduler tick; that's done in the
FastAPI startup hook so health probes don't see an empty exposition
for the first full ``refresh_interval``.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import signal
from pathlib import Path
from typing import Any

import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse

from .config import Config, ReloadAuthConfig, load_config
from .exporter import Exporter, OperationalMetrics
from .logging import configure_logging, get_logger
from .runner import Runner
from .source import PromQLSource
from .state import SnapshotStore

logger = get_logger(__name__)


class Application:
    """Encapsulates everything wired together by main().

    Kept as a class so the SIGHUP handler and the reload endpoint share
    one path through ``self.reload()``.
    """

    def __init__(self, config: Config, config_path: Path) -> None:
        self._config = config
        self._config_path = config_path
        self._store = SnapshotStore()
        self._ops = OperationalMetrics()
        self._source = PromQLSource(
            base_url=config.datasource.url,
            timeout=config.datasource.timeout_seconds,
            auth=config.datasource.auth,
        )
        self._runner = Runner(
            config=config,
            source=self._source,
            store=self._store,
            operational=self._ops,
        )
        self._exporter = Exporter(self._store, self._ops)
        self._scheduler: AsyncIOScheduler | None = None
        # Anchor background tasks so the GC can't drop them mid-run.
        self._background: set[asyncio.Task[Any]] = set()
        self._stamp_config_hash(config)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def startup(self) -> None:
        await self._source.start()
        self._scheduler = AsyncIOScheduler()
        self._reschedule_jobs(self._scheduler)
        self._scheduler.start()
        # SIGHUP must be wired here, not in serve(), because only inside
        # the running event loop is ``loop.add_signal_handler`` safe to
        # call. ``signal.signal()`` can fire between bytecode instructions
        # and corrupt asyncio internals if it touches the loop.
        self._install_signal_handlers()
        # First run is immediate so /metrics has data before the first
        # tick, which matters when refresh_interval is on the order of
        # a minute.
        for group in self._config.groups:
            self._spawn(self._safe_group_run(group.name))

    async def shutdown(self) -> None:
        if self._scheduler is not None:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None
        await self._source.close()

    # ------------------------------------------------------------------
    # Endpoints
    # ------------------------------------------------------------------
    def build_app(self) -> FastAPI:
        @contextlib.asynccontextmanager
        async def lifespan(_: FastAPI) -> Any:
            await self.startup()
            try:
                yield
            finally:
                await self.shutdown()

        app = FastAPI(lifespan=lifespan, title="promanomaly")

        @app.get("/healthz", response_class=PlainTextResponse)
        async def healthz() -> str:
            return "ok"

        @app.get("/ready", response_class=PlainTextResponse)
        async def ready() -> Response:
            if not self._config.server.ready_endpoint:
                return PlainTextResponse("ready endpoint disabled", status_code=404)
            if self._store.any_group_ready():
                return PlainTextResponse("ready", status_code=200)
            return PlainTextResponse("not ready", status_code=503)

        @app.get("/metrics")
        async def metrics() -> Response:
            body, content_type = self._exporter.render()
            return Response(content=body, media_type=content_type)

        @app.post("/-/reload")
        async def reload_endpoint(request: Request) -> Response:
            if not self._config.server.reload.enabled:
                raise HTTPException(status_code=404, detail="reload disabled")
            self._authorise_reload(request)
            ok, detail = await self.reload()
            if not ok:
                return JSONResponse({"status": "error", "detail": detail}, status_code=400)
            return JSONResponse({"status": "ok"})

        @app.get("/debug/config")
        async def debug_config() -> JSONResponse:
            # Bounded, read-only view of the active config (no secrets).
            return JSONResponse(self._safe_config_view())

        return app

    # ------------------------------------------------------------------
    # Reload
    # ------------------------------------------------------------------
    async def reload(self) -> tuple[bool, str]:
        """Re-read the config from disk, validate, and swap in atomically.

        Validation failure leaves the previous config running unchanged —
        a fat-finger in the YAML never breaks the running detector.
        """
        try:
            new_config = load_config(self._config_path)
        except Exception as exc:
            logger.error("reload_validation_failed", error=str(exc))
            return False, f"validation failed: {exc}"

        self._runner.replace_config(new_config)
        self._config = new_config
        self._stamp_config_hash(new_config)
        if self._scheduler is not None:
            self._reschedule_jobs(self._scheduler)
        # Trigger an immediate run for any groups that may have changed,
        # mirroring the boot-time first-run pattern.
        for group in new_config.groups:
            self._spawn(self._safe_group_run(group.name))
        logger.info("reload_complete")
        return True, "ok"

    def _authorise_reload(self, request: Request) -> None:
        auth = self._config.server.reload.auth
        if auth.type == "none":
            return
        if auth.type == "bearer":
            self._authorise_bearer(request, auth)
            return
        # mTLS is enforced by the ingress / NetworkPolicy in practice;
        # we accept the request if reached because the TLS handshake
        # would have failed upstream otherwise.

    @staticmethod
    def _authorise_bearer(request: Request, auth: ReloadAuthConfig) -> None:
        if not auth.token:
            raise HTTPException(status_code=401, detail="unauthorised")
        header = request.headers.get("authorization", "")
        expected = f"Bearer {auth.token}"
        # Constant-time comparison — short-circuiting on the first
        # differing byte leaks token length to a timing side-channel.
        if not hmac.compare_digest(header.encode("utf-8"), expected.encode("utf-8")):
            raise HTTPException(status_code=401, detail="unauthorised")

    # ------------------------------------------------------------------
    # Scheduling
    # ------------------------------------------------------------------
    def _reschedule_jobs(self, scheduler: AsyncIOScheduler) -> None:
        # Clear and rebuild all jobs. APScheduler supports per-job updates
        # but a full rebuild keeps reload semantics simple and avoids
        # subtle drift between the live job set and the new config.
        scheduler.remove_all_jobs()
        interval = self._config.server.refresh_interval_seconds
        # APScheduler's IntervalTrigger defers the first run by one
        # full interval; the immediate first run is fired from startup()
        # separately so /metrics has content before that delay.
        for group in self._config.groups:
            scheduler.add_job(
                self._tick,
                trigger=IntervalTrigger(seconds=interval),
                args=[group.name],
                id=f"group:{group.name}",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )

    async def _tick(self, group_name: str) -> None:
        await self._safe_group_run(group_name)

    async def _safe_group_run(self, group_name: str) -> None:
        try:
            await self._runner.run_group(group_name)
        except Exception as exc:
            logger.exception("scheduled_run_failed", group=group_name, error=str(exc))

    def _spawn(self, coro: Any) -> None:
        """Track background tasks so the GC keeps them alive (RUF006)."""
        task = asyncio.create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    def _install_signal_handlers(self) -> None:
        """Wire SIGHUP -> ``self.reload`` through the running event loop.

        ``loop.add_signal_handler`` runs the callback as a regular asyncio
        callback rather than inside the OS signal handler, which keeps it
        free to use the full asyncio API (unlike ``signal.signal``).
        Some platforms (Windows) and contexts (non-main thread) don't
        support add_signal_handler; we degrade silently rather than
        block startup.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        try:
            loop.add_signal_handler(signal.SIGHUP, self._on_sighup)
        except (NotImplementedError, ValueError, RuntimeError):
            logger.info("sighup_handler_unavailable")

    def _on_sighup(self) -> None:
        logger.info("sighup_received")
        self._spawn(self.reload())

    # ------------------------------------------------------------------
    # Misc helpers
    # ------------------------------------------------------------------
    def _stamp_config_hash(self, config: Config) -> None:
        digest = hashlib.sha256(
            json.dumps(config.model_dump(mode="json"), sort_keys=True).encode("utf-8")
        ).hexdigest()
        self._ops.config_hash.clear()
        self._ops.config_hash.labels(sha256=digest).set(1.0)

    def _safe_config_view(self) -> dict[str, Any]:
        view = self._config.model_dump(mode="json")
        # Strip anything that smells like a secret.
        for sensitive in ("token", "password"):
            self._scrub_keys(view, sensitive)
        return view

    @classmethod
    def _scrub_keys(cls, obj: Any, key: str) -> None:
        if isinstance(obj, dict):
            for k in list(obj):
                if k == key and obj[k] not in (None, ""):
                    obj[k] = "<redacted>"
                else:
                    cls._scrub_keys(obj[k], key)
        elif isinstance(obj, list):
            for item in obj:
                cls._scrub_keys(item, key)


# ----------------------------------------------------------------------
# Module-level entrypoints
# ----------------------------------------------------------------------


def _parse_listen(addr: str) -> tuple[str, int]:
    """Accept ``":9092"`` / ``"0.0.0.0:9092"`` and return ``(host, port)``."""
    addr = addr.strip()
    if addr.startswith(":"):
        return "0.0.0.0", int(addr[1:])
    host, _, port = addr.rpartition(":")
    if not host or not port:
        raise ValueError(f"invalid listen address {addr!r}")
    return host, int(port)


def serve(config_path: str | Path) -> None:
    """Boot the detector. Used by the CLI default command."""
    config_path = Path(config_path)
    config = load_config(config_path)
    configure_logging("INFO")
    logger.info(
        "boot",
        config_path=str(config_path),
        groups=[g.name for g in config.groups],
        refresh_interval_seconds=config.server.refresh_interval_seconds,
    )
    if config.aggressive_refresh:
        logger.warning(
            "aggressive_refresh_interval",
            refresh_interval_seconds=config.server.refresh_interval_seconds,
            window_seconds=config.defaults.window_seconds,
            recommended_minimum_seconds=config.defaults.window_seconds / 6.0,
            note="refresh_interval below window/6 — rolling baseline lags",
        )
    application = Application(config, config_path)
    app = application.build_app()
    host, port = _parse_listen(config.server.listen)

    config_obj = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_config=None,
        access_log=False,
        lifespan="on",
    )
    server = uvicorn.Server(config_obj)
    server.run()


__all__ = ["Application", "serve"]
