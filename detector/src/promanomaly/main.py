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
import hashlib
import json
import signal
from pathlib import Path
from typing import Any

import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI

from .cache import RedisQueryCache, TTLCache
from .config import Config, load_config
from .exporter import Exporter, OperationalMetrics
from .ha import HAComponents, build_ha_components, run_follower_loop, stop_ha
from .leader import resolve_identity
from .logging import configure_logging, get_logger
from .runner import Runner
from .server import build_app as _build_app
from .source import PromQLSource, QueryResult
from .state import SnapshotStore
from .telemetry import Telemetry

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
        # OpenTelemetry tracing — built from config exactly once at boot.
        # Reload swaps in a fresh Telemetry via ``Runner._telemetry`` only
        # when the endpoint actually changed; for now any reload that
        # touches telemetry config requires a restart, which is documented
        # in docs/telemetry.md alongside the zero-overhead caveat.
        self._telemetry = Telemetry.from_config(config.telemetry)
        self._runner = Runner(
            config=config,
            source=self._source,
            store=self._store,
            operational=self._ops,
            telemetry=self._telemetry,
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
        self._ha: HAComponents | None = None
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
        # Serialises reload() so a rapid SIGHUP + /-/reload pair cannot
        # race two coroutines through config load + swap. Built lazily
        # in reload() because asyncio.Lock binds to the running loop and
        # Application can be constructed outside one (tests).
        self._reload_lock: asyncio.Lock | None = None
        # Guards the leader-check + snapshot-publish sequence so a task
        # that finished run_group cannot publish after demotion flipped
        # the flag. ``_on_stopped_leading`` flips ``_is_leader`` under
        # the same lock before cancellation; once flipped, any in-flight
        # publisher waiting here sees ``_is_leader=False`` and skips.
        self._publish_lock: asyncio.Lock | None = None

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
        await stop_ha(self._ha)
        self._ha = None
        if self._scheduler is not None:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None
        await self._source.close()
        # Shut down the runner's thread pool so pending detector
        # invocations don't outlive the process. wait=False mirrors the
        # scheduler behaviour: in-flight detection runs are abandoned
        # rather than allowed to complete (matching the per-detector
        # cancellation contract documented in runner.py).
        self._runner._executor.shutdown(wait=False)
        # Flush buffered OpenTelemetry spans before the process exits.
        # BatchSpanProcessor queues spans on a background thread; without
        # the explicit shutdown the last batch is dropped on rolling
        # restarts. No-op when telemetry was never enabled.
        self._telemetry.shutdown()

    def build_app(self) -> FastAPI:
        """Build the FastAPI app for this Application.

        Thin wrapper around :func:`promanomaly.server.build_app`; the
        route definitions live there so the HTTP surface stays
        testable independent of the scheduler / HA wiring.
        """
        return _build_app(self)

    # ------------------------------------------------------------------
    # Reload
    # ------------------------------------------------------------------
    async def reload(self) -> tuple[bool, str]:
        """Re-read the config from disk, validate, and swap in atomically.

        Validation failure leaves the previous config running unchanged —
        a fat-finger in the YAML never breaks the running detector. The
        lock serialises concurrent reload attempts (rapid SIGHUP +
        ``/-/reload``) so the second arrival sees the already-swapped
        config rather than racing on disk reads.
        """
        if self._reload_lock is None:
            self._reload_lock = asyncio.Lock()
        async with self._reload_lock:
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
            # Trigger an immediate run for any groups that may have
            # changed, mirroring the boot-time first-run pattern.
            for group in new_config.groups:
                self._spawn(self._safe_group_run(group.name))
            logger.info("reload_complete")
            return True, "ok"

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
            # is only built when HA is enabled). The publish_lock makes
            # the leader-check + publish atomic with respect to
            # ``_on_stopped_leading``: once demotion has flipped
            # ``_is_leader`` under the lock, any task waiting here on
            # the same lock observes the new value and skips the publish.
            if result.succeeded and self._ha is not None:
                snapshot = self._store.get(group_name)
                if snapshot is not None:
                    lock = self._ensure_publish_lock()
                    async with lock:
                        if not self._ha_enabled or self._is_leader:
                            self._ha.snapshot_cache.publish(snapshot)
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
        self._ha = build_ha_components(
            config=self._config.highAvailability,
            redis_config=self._config.safety.redis,
            redis_client=self._redis,
            store=self._store,
            identity=self._identity,
            on_started_leading=self._on_started_leading,
            on_stopped_leading=self._on_stopped_leading,
        )
        self._ha.elector.start()
        self._ha.follower_task = asyncio.create_task(
            run_follower_loop(
                self._ha.follower_syncer,
                self._config.server.refresh_interval_seconds / 2.0,
            )
        )

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
        # In HA mode the Runner skips boot-time stratified-config
        # warnings so every follower doesn't log the same line. The
        # leader surfaces them on each acquisition — re-fires on
        # failover so the new leader still gets a one-time surface
        # without N-replicas-worth of noise.
        self._runner.surface_misconfigurations()
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
        # Flip the leader flag under the publish lock so a task already
        # past run_group but waiting on the lock sees False and skips
        # its publish. Without this guard the synchronous test+publish
        # sequence between ``_is_leader`` and ``snapshot_cache.publish``
        # could slip through after demotion.
        lock = self._ensure_publish_lock()
        async with lock:
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

    def _ensure_publish_lock(self) -> asyncio.Lock:
        # asyncio.Lock binds to the running loop, so we can't build it
        # in __init__ (Application is constructed in tests outside a
        # loop). Lazily build on first use; safe because callers are
        # always inside the runner's event loop.
        if self._publish_lock is None:
            self._publish_lock = asyncio.Lock()
        return self._publish_lock

    # ------------------------------------------------------------------
    # Misc helpers
    # ------------------------------------------------------------------
    def _stamp_config_hash(self, config: Config) -> None:
        digest = hashlib.sha256(
            json.dumps(config.model_dump(mode="json"), sort_keys=True).encode("utf-8")
        ).hexdigest()
        self._ops.config_hash.clear()
        self._ops.config_hash.labels(sha256=digest).set(1.0)


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
    app = _build_app(application)
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
