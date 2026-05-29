"""HA-mode helpers extracted from :mod:`promanomaly.main`.

Encapsulates the bits of leader election + follower-poll wiring that
don't touch the :class:`Application` task tracker or per-group run
state. The leadership callbacks themselves stay on Application
because they cancel in-flight tasks via the application's task
registry — moving them out would require threading the registry
through another indirection without simplifying anything.

This module owns:

- :class:`HAComponents` — bundle of the leader elector, the shared
  Redis snapshot cache, and the follower syncer.
- :func:`build_ha_components` — wires the three together.
- :func:`run_follower_loop` — the periodic snapshot-poll loop.
- :func:`stop_ha` — graceful teardown of the elector + follower task.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .config import HighAvailabilityConfig, RedisConfig
from .leader import LeaderElector, LeaseClient
from .logging import get_logger
from .state import (
    FollowerSyncer,
    RedisDiscoveryMissTracker,
    SharedSnapshotCache,
    SnapshotStore,
)

logger = get_logger(__name__)


@dataclass
class HAComponents:
    """The elector + shared cache + follower syncer for one Application."""

    elector: LeaderElector
    snapshot_cache: SharedSnapshotCache
    follower_syncer: FollowerSyncer
    discovery_tracker: RedisDiscoveryMissTracker
    follower_task: asyncio.Task[None] | None = field(default=None)


def build_ha_components(
    *,
    config: HighAvailabilityConfig,
    redis_config: RedisConfig,
    redis_client: Any,
    store: SnapshotStore,
    identity: str,
    refresh_interval_seconds: float,
    on_started_leading: Callable[[], Awaitable[None]],
    on_stopped_leading: Callable[[], Awaitable[None]],
) -> HAComponents:
    """Wire the leader elector + shared cache + follower syncer.

    ``redis_client`` is required (HA is built on Redis); the caller is
    expected to have already constructed it. ``on_started_leading`` /
    ``on_stopped_leading`` are application-supplied coroutines invoked
    by the elector on Lease acquisition / loss — kept as callbacks
    because they touch Application state that doesn't belong here.
    """
    snapshot_cache = SharedSnapshotCache(
        client=redis_client,
        ttl_seconds=config.snapshot_ttl_seconds,
        key_prefix=f"{redis_config.key_prefix}:snapshot",
    )
    follower_syncer = FollowerSyncer(store=store, shared=snapshot_cache)
    # Discovery-absence miss counts live in Redis so they survive a Lease
    # failover (the in-memory tracker would reset the grace window on the
    # new leader). The TTL is refreshed on every write, so while a leader
    # writes each refresh the key never expires; it only matters for the
    # failover gap and orphan cleanup, so a few refresh intervals (floored
    # at the snapshot TTL) is ample.
    discovery_ttl = max(config.snapshot_ttl_seconds, refresh_interval_seconds * 4.0)
    discovery_tracker = RedisDiscoveryMissTracker(
        client=redis_client,
        ttl_seconds=discovery_ttl,
        key_prefix=f"{redis_config.key_prefix}:discovery",
    )
    elector = LeaderElector(
        ha=config,
        identity=identity,
        client=LeaseClient(
            namespace=config.lease_namespace,
            name=config.lease_name,
            lease_duration_seconds=int(config.lease_duration_seconds),
        ),
        on_started_leading=on_started_leading,
        on_stopped_leading=on_stopped_leading,
    )
    return HAComponents(
        elector=elector,
        snapshot_cache=snapshot_cache,
        follower_syncer=follower_syncer,
        discovery_tracker=discovery_tracker,
    )


async def run_follower_loop(syncer: FollowerSyncer, interval_seconds: float) -> None:
    """Poll the shared snapshot cache and refresh the local store.

    Sleeps ``interval_seconds`` between polls so followers see a new
    snapshot within one polling interval of the leader publishing it.
    Errors are logged and the loop continues — a Redis blip leaves
    followers serving the previous snapshot rather than going blank.
    """
    interval = max(1.0, interval_seconds)
    while True:
        try:
            await asyncio.to_thread(syncer.poll_once)
        except Exception as exc:
            logger.warning("follower_poll_failed", error=str(exc))
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return


async def stop_ha(components: HAComponents | None) -> None:
    """Gracefully tear down the elector and follower task.

    Idempotent; safe to call on a never-started Application
    (``components`` is None outside HA mode).
    """
    if components is None:
        return
    await components.elector.stop()
    if components.follower_task is not None:
        components.follower_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await components.follower_task
        components.follower_task = None


__all__ = [
    "HAComponents",
    "build_ha_components",
    "run_follower_loop",
    "stop_ha",
]
