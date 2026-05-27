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

from .cache import RedisQueryCache, TTLCache
from .config import Config, ReloadAuthConfig, load_config
from .detectors import list_meta
from .exporter import Exporter, OperationalMetrics
from .inspect import InspectError, inspect_series
from .leader import LeaderElector, LeaseClient, resolve_identity
from .logging import configure_logging, get_logger
from .runner import Runner
from .source import PromQLSource, QueryResult
from .state import FollowerSyncer, SharedSnapshotCache, SnapshotStore

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
        self._redis = _maybe_build_redis(config)
        self._source = PromQLSource(
            base_url=config.datasource.url,
            timeout=config.datasource.timeout_seconds,
            auth=config.datasource.auth,
            cache=_build_query_cache(config, self._redis),
        )
        self._runner = Runner(
            config=config,
            source=self._source,
            store=self._store,
            operational=self._ops,
        )
        self._exporter = Exporter(self._store, self._ops, self._source)
        self._scheduler: AsyncIOScheduler | None = None
        # Anchor background tasks so the GC can't drop them mid-run.
        self._background: set[asyncio.Task[Any]] = set()
        # HA wiring: built lazily inside startup() so unit tests that
        # construct Application without a Kubernetes cluster keep
        # working. ``_is_leader`` defaults to True in non-HA mode so
        # the scheduler path is identical to the single-replica MVP.
        self._ha_enabled = config.highAvailability.enabled
        self._is_leader = not self._ha_enabled
        self._elector: LeaderElector | None = None
        self._snapshot_cache: SharedSnapshotCache | None = None
        self._follower_syncer: FollowerSyncer | None = None
        self._follower_task: asyncio.Task[None] | None = None
        # Per-group in-flight detection tasks. Holding the references
        # lets ``_on_stopped_leading`` cancel them on demotion so a
        # newly elected leader can't race with a write that started on
        # the previous leader. A set per group (rather than a single
        # task) handles the edge case where a rapidly-toggled Lease or
        # an interleaved reload spawns a second run for the same group
        # while the previous one is still waiting on the per-group lock
        # in the runner; without a set, the newer registration would
        # silently shadow the older task and cancellation would miss
        # it. Single-replica leaves this dict empty.
        self._running_group_tasks: dict[str, set[asyncio.Task[None]]] = {}
        self._identity = resolve_identity(config.highAvailability.identity)
        self._stamp_config_hash(config)
        if self._ha_enabled:
            self._ops.leader_elected.labels(instance=self._identity).set(0.0)

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
        if self._ha_enabled:
            await self._start_ha()
        else:
            # Single-replica: kick off the first run immediately so
            # /metrics has data before the first scheduler tick.
            for group in self._config.groups:
                self._spawn(self._safe_group_run(group.name))

    async def shutdown(self) -> None:
        if self._elector is not None:
            await self._elector.stop()
            self._elector = None
        if self._follower_task is not None:
            self._follower_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._follower_task
            self._follower_task = None
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

        @app.get("/debug/detectors")
        async def debug_detectors() -> JSONResponse:
            # Snapshot of the detector entry-point registry. Stable shape
            # so the CLI ``detectors list --target`` can render it
            # without negotiation; described in docs/detectors.md.
            # ``defaults`` is a simple {name: default} dict kept for
            # backwards compatibility; ``params`` is the canonical richer
            # form with per-parameter type and doc.
            return JSONResponse([m.as_dict() for m in list_meta()])

        @app.get("/debug/inspect")
        async def debug_inspect(
            id: str,
            labels: str | None = None,
        ) -> JSONResponse:
            # Live introspection for one (id, label-selector) series.
            # Refetches the source query so the operator sees the same
            # rolling window the detector would see right now. Bounded
            # cost by the source-query budget and the per-detector
            # compute cost — the NetworkPolicy shipped with the Helm
            # chart denies ingress to /debug/* by default so this is
            # only reachable inside the namespace.
            try:
                payload = await inspect_series(
                    config=self._config,
                    source=self._source,
                    store=self._store,
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
        self._source.replace_cache(_build_query_cache(new_config, self._redis))
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
        # In HA mode followers must never run the detector pipeline —
        # only the elected leader writes snapshots. Polling the leader
        # flag here (rather than gating in run_group) keeps the
        # scheduler still useful: ticks fire on every replica but the
        # work is no-op'd on followers, which keeps the timing
        # behaviour identical when the leader changes mid-flight.
        if self._ha_enabled and not self._is_leader:
            return
        await self._safe_group_run(group_name)

    async def _safe_group_run(self, group_name: str) -> None:
        # Register the task so a leadership loss can cancel it. We do
        # this even in single-replica mode (no-op there) to keep the
        # path uniform; the set stays tiny (typically one entry per
        # group, occasionally two during rapid re-spawns).
        task = asyncio.current_task()
        if task is not None:
            self._running_group_tasks.setdefault(group_name, set()).add(task)
        try:
            try:
                result = await self._runner.run_group(group_name)
            except asyncio.CancelledError:
                # Cancellation during leadership loss: don't publish
                # anything, don't log as a failure, just unwind cleanly.
                logger.info("group_run_cancelled", group=group_name)
                raise
            except Exception as exc:
                logger.exception("scheduled_run_failed", group=group_name, error=str(exc))
                return
            # Publish to the shared cache so followers see the same data.
            # In single-replica mode this branch is skipped (snapshot_cache
            # is only built when HA is enabled). The leader check guards
            # against a publish racing with a demotion that happened
            # mid-run — the cooldown is the primary defence but the flag
            # check is cheap insurance.
            if (
                result.succeeded
                and self._snapshot_cache is not None
                and (not self._ha_enabled or self._is_leader)
            ):
                snapshot = self._store.get(group_name)
                if snapshot is not None:
                    self._snapshot_cache.publish(snapshot)
        finally:
            if task is not None:
                tasks = self._running_group_tasks.get(group_name)
                if tasks is not None:
                    tasks.discard(task)
                    if not tasks:
                        self._running_group_tasks.pop(group_name, None)

    def _spawn(self, coro: Any) -> None:
        """Track background tasks so the GC keeps them alive (RUF006)."""
        task = asyncio.create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    # ------------------------------------------------------------------
    # HA mode
    # ------------------------------------------------------------------
    async def _start_ha(self) -> None:
        """Spin up the leader elector and the follower-poll loop.

        Both sides run on every replica: the elector decides leadership,
        and the follower loop keeps the local snapshot store in sync
        with the leader's published snapshots so any replica can serve
        ``/metrics``. The follower loop is a no-op on the leader (its
        own writes already populated the store) but kept running so a
        demoted leader transparently switches to read-only mode.
        """
        if self._redis is None:
            raise RuntimeError("highAvailability.enabled requires safety.redis to be configured")
        self._snapshot_cache = SharedSnapshotCache(
            client=self._redis,
            ttl_seconds=self._config.highAvailability.snapshot_ttl_seconds,
            key_prefix=f"{self._config.safety.redis.key_prefix}:snapshot",
        )
        self._follower_syncer = FollowerSyncer(store=self._store, shared=self._snapshot_cache)
        ha = self._config.highAvailability
        self._elector = LeaderElector(
            ha=ha,
            identity=self._identity,
            client=LeaseClient(
                namespace=ha.lease_namespace,
                name=ha.lease_name,
                lease_duration_seconds=int(ha.lease_duration_seconds),
            ),
            on_started_leading=self._on_started_leading,
            on_stopped_leading=self._on_stopped_leading,
        )
        self._elector.start()
        self._follower_task = asyncio.create_task(self._follower_loop())

    async def _on_started_leading(self) -> None:
        """Become leader: kick off an immediate run for every group.

        Documented failover behaviour from the project guide — without
        this the freshness gap during failover would be one full
        ``refresh_interval``. Followers keep serving the previous
        leader's snapshot from Redis until this leader's first runs
        complete and publish.
        """
        self._is_leader = True
        self._ops.leader_elected.labels(instance=self._identity).set(1.0)
        self._ops.leader_transitions_total.labels(instance=self._identity).inc()
        logger.info("started_leading", identity=self._identity)
        for group in self._config.groups:
            self._spawn(self._safe_group_run(group.name))

    async def _on_stopped_leading(self) -> None:
        """Stop being leader: cancel in-flight runs and cool down.

        Cancellation matches the documented contract: in-flight
        detection runs are abandoned rather than allowed to publish
        after we've lost the Lease. Cooldown caps any further write
        windows at ``refresh_interval / 2`` per the project guide.
        The newly-elected leader will reproduce the same scores from
        the same source on its own immediate first run.
        """
        self._is_leader = False
        self._ops.leader_elected.labels(instance=self._identity).set(0.0)
        cancelled: list[asyncio.Task[None]] = [
            task for tasks in self._running_group_tasks.values() for task in tasks
        ]
        for task in cancelled:
            task.cancel()
        if cancelled:
            # Await the cancellation to drain so the cooldown is a
            # real "no-more-writes" guarantee rather than a hopeful
            # sleep racing the in-flight code.
            await asyncio.gather(*cancelled, return_exceptions=True)
        cooldown = self._config.server.refresh_interval_seconds / 2.0
        logger.info(
            "stopped_leading",
            identity=self._identity,
            cooldown_seconds=cooldown,
            cancelled_tasks=len(cancelled),
        )
        await asyncio.sleep(cooldown)

    async def _follower_loop(self) -> None:
        """Poll the shared snapshot cache and refresh the local store.

        Sleeps ``refresh_interval / 2`` between polls so followers see a
        new snapshot within one polling interval of the leader publishing
        it. Errors are logged and the loop continues — a Redis blip
        leaves followers serving the previous snapshot rather than going
        blank.
        """
        if self._follower_syncer is None:
            return
        interval = max(1.0, self._config.server.refresh_interval_seconds / 2.0)
        while True:
            try:
                await asyncio.to_thread(self._follower_syncer.poll_once)
            except Exception as exc:
                logger.warning("follower_poll_failed", error=str(exc))
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                return

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


def _build_query_cache(
    config: Config,
    redis_client: Any | None = None,
) -> Any:
    """Return a query-cache backend honoured by the source layer, or ``None``.

    Returns one of:

    - ``None`` when ``safety.query_cache.enabled=false``.
    - :class:`TTLCache` (default) — in-process LRU+TTL.
    - :class:`RedisQueryCache` — when ``backend: redis`` is configured;
      shared by leader and followers in HA mode.
    """
    cfg = config.safety.query_cache
    if not cfg.enabled:
        return None
    if cfg.backend == "redis":
        if redis_client is None:
            raise RuntimeError(
                "safety.query_cache.backend=redis requires safety.redis to be configured"
            )
        return RedisQueryCache(
            client=redis_client,
            ttl_seconds=cfg.ttl_seconds,
            key_prefix=f"{config.safety.redis.key_prefix}:query",
        )
    return TTLCache[tuple[str, float, float, int, int], QueryResult](
        max_entries=cfg.max_entries, ttl_seconds=cfg.ttl_seconds
    )


def _maybe_build_redis(config: Config) -> Any | None:
    """Construct a Redis client iff any consumer in the config needs one.

    HA mode and ``safety.query_cache.backend=redis`` both share one
    Redis instance. When neither is configured we don't even import
    ``redis`` to keep boot fast for single-replica deployments.
    """
    needs_redis = config.highAvailability.enabled or config.safety.query_cache.backend == "redis"
    if not needs_redis:
        return None
    from redis import Redis

    redis_cfg = config.safety.redis
    return Redis.from_url(
        redis_cfg.url,
        socket_timeout=redis_cfg.timeout_seconds,
        socket_connect_timeout=redis_cfg.timeout_seconds,
    )


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
