"""Integration tests for the HA wiring inside ``main.Application``.

The pure-elector tests in ``test_leader_election.py`` only cover
``LeaderElector``'s API. These tests cover the Application-level
behaviour the project guide and docs promise:

- Followers don't run the detector pipeline (``_tick`` no-ops when not leader).
- An elected leader fires an immediate run for every group rather than
  waiting for the first scheduler tick.
- Successful runs publish to the shared Redis snapshot cache so
  followers can pull them.
- A demoted leader cancels in-flight runs before any further snapshot
  writes can land.

The Kubernetes Lease client is mocked via the ``LeaseClient`` interface
so the suite never reaches out to a cluster; the Redis client is
``fakeredis``.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import fakeredis
import pytest
import yaml

from promanomaly.config import CURRENT_API_VERSION, load_config
from promanomaly.main import Application
from promanomaly.state import GroupSnapshot, Sample
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline


def _ha_yaml(tmp_path: Path) -> Path:
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {
            "listen": ":0",
            "refresh_interval": "30s",
            "ready_endpoint": True,
        },
        "safety": {
            "redis": {"url": "redis://stub-redis:6379/0"},
        },
        "highAvailability": {
            "enabled": True,
            "lease_name": "test-lease",
            "lease_namespace": "ns",
            "snapshot_ttl": "60s",
        },
        "defaults": {
            "window": "5m",
            "step": "15s",
            "min_points": 5,
            "warmup_policy": "emit_warming_up",
            "emit_baseline_stability": False,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {"id": "m1", "promql": "up", "detectors": [{"name": "MAD"}]},
                ],
            }
        ],
    }
    path = tmp_path / "ha.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


@pytest.fixture
def ha_app(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Application:
    """An Application configured for HA, with PromQL + Redis stubbed.

    Critically: we do NOT call ``startup()``. That would spin up the
    real ``LeaderElector`` which tries to talk to a Kubernetes cluster.
    Instead the tests directly exercise the bits of Application that
    HA mode changes (``_tick``, ``_safe_group_run``,
    ``_on_started_leading``, ``_on_stopped_leading``).
    """
    config_path = _ha_yaml(tmp_path)
    stub = StubSource()
    stub.respond(
        lambda _: StubResponse(series=[make_series({"instance": "h"}, clean_baseline(n=120))])
    )
    monkeypatch.setattr("promanomaly.main.PromQLSource", lambda **_: stub)
    # Replace _maybe_build_redis so the Application's Redis client is
    # a fakeredis instance instead of a real connection.
    fake = fakeredis.FakeStrictRedis()
    monkeypatch.setattr("promanomaly.main._maybe_build_redis", lambda _cfg: fake)
    app = Application(load_config(config_path), config_path)
    # Build the shared snapshot cache lazily — same as _start_ha() does —
    # so the snapshot-publish path is wired without launching the elector.
    # The other HAComponents fields are mocked because the publish path
    # only consults ``snapshot_cache``.
    from unittest.mock import MagicMock

    from promanomaly.ha import HAComponents
    from promanomaly.state import RedisDiscoveryMissTracker, SharedSnapshotCache

    discovery_tracker = RedisDiscoveryMissTracker(
        client=fake, ttl_seconds=60.0, key_prefix="promanomaly:discovery"
    )
    app._ha = HAComponents(
        elector=MagicMock(),
        snapshot_cache=SharedSnapshotCache(
            client=fake,
            ttl_seconds=60.0,
            key_prefix="promanomaly:snapshot",
        ),
        follower_syncer=MagicMock(),
        discovery_tracker=discovery_tracker,
    )
    # The store swap normally happens in _start_ha(); the fixture wires
    # _ha by hand (without launching the elector), so mirror it here so
    # the discovery-absence path uses the Redis-backed tracker.
    app._store.set_discovery_tracker(discovery_tracker)
    return app


@pytest.mark.asyncio
async def test_tick_does_nothing_on_followers(ha_app: Application) -> None:
    """Coverage gap #10: a follower must not execute the detector
    pipeline. The scheduler still fires ticks on every replica — they're
    gated inside ``_tick`` by the leader flag."""
    # HA-enabled Application boots as follower (_is_leader=False).
    assert not ha_app._is_leader
    await ha_app._tick("g1")
    snap = ha_app._store.get("g1")
    assert snap is None, "follower wrote a snapshot — leader gating broke"


@pytest.mark.asyncio
async def test_on_started_leading_triggers_immediate_run_for_each_group(
    ha_app: Application,
) -> None:
    """Coverage gap #11: leader-acquire fires an immediate run for every
    configured group, so /metrics doesn't sit empty for a full
    ``refresh_interval`` after failover."""
    await ha_app._on_started_leading()
    # Wait briefly for the spawned tasks to complete.
    for _ in range(50):
        if ha_app._store.get("g1") is not None:
            break
        await asyncio.sleep(0.02)
    snap = ha_app._store.get("g1")
    assert snap is not None
    assert ha_app._is_leader is True


@pytest.mark.asyncio
async def test_safe_group_run_publishes_snapshot_to_shared_cache(
    ha_app: Application,
) -> None:
    """A successful run on the leader publishes the snapshot to the
    shared cache so followers can pull it."""
    ha_app._is_leader = True
    await ha_app._safe_group_run("g1")
    assert ha_app._ha is not None
    loaded = ha_app._ha.snapshot_cache.load_all()
    assert len(loaded) == 1
    assert loaded[0].group == "g1"


@pytest.mark.asyncio
async def test_on_stopped_leading_cancels_in_flight_runs(
    monkeypatch: pytest.MonkeyPatch, ha_app: Application
) -> None:
    """Debt #6: when leadership is lost, an in-flight detection run is
    cancelled before it can publish — the previous leader does not
    race the new one for a shared-cache write."""
    ha_app._is_leader = True

    # Patch the runner so its run_group blocks long enough for us to
    # trigger the demotion mid-flight.
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def slow_run_group(group_name: str) -> Any:
        started.set()
        try:
            await asyncio.sleep(10.0)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return None

    monkeypatch.setattr(ha_app._runner, "run_group", slow_run_group)

    # Fire the run as a background task.
    run_task = asyncio.create_task(ha_app._safe_group_run("g1"))
    await asyncio.wait_for(started.wait(), timeout=1.0)
    assert run_task in ha_app._running_group_tasks["g1"]

    # Shorten the cooldown so the test doesn't sit through the full
    # refresh_interval / 2.
    ha_app._config.server.refresh_interval = "0.1s"
    await ha_app._on_stopped_leading()

    assert cancelled.is_set()
    assert "g1" not in ha_app._running_group_tasks
    # The cancellation propagated out of the run task too.
    with pytest.raises(asyncio.CancelledError):
        run_task.result()


@pytest.mark.asyncio
async def test_concurrent_runs_for_same_group_both_cancelled(
    monkeypatch: pytest.MonkeyPatch, ha_app: Application
) -> None:
    """Audit fix: when ``_safe_group_run`` is invoked twice for the
    same group while the first is still in flight (e.g. rapid Lease
    churn), cancellation on demotion must reach BOTH tasks, not just
    the most recently registered one."""
    ha_app._is_leader = True

    started_count = 0
    cancelled_count = 0
    barrier = asyncio.Event()

    async def slow_run_group(_group_name: str) -> Any:
        nonlocal started_count, cancelled_count
        started_count += 1
        try:
            await barrier.wait()
        except asyncio.CancelledError:
            cancelled_count += 1
            raise
        return None

    monkeypatch.setattr(ha_app._runner, "run_group", slow_run_group)

    task1 = asyncio.create_task(ha_app._safe_group_run("g1"))
    task2 = asyncio.create_task(ha_app._safe_group_run("g1"))
    # Give both tasks time to register themselves.
    for _ in range(50):
        if started_count == 2:
            break
        await asyncio.sleep(0.01)
    assert len(ha_app._running_group_tasks["g1"]) == 2

    ha_app._config.server.refresh_interval = "0.1s"
    await ha_app._on_stopped_leading()

    assert cancelled_count == 2
    # Drain the wrappers so the test event loop is clean.
    for t in (task1, task2):
        with pytest.raises(asyncio.CancelledError):
            t.result()


@pytest.mark.asyncio
async def test_in_flight_run_does_not_publish_after_demotion(
    monkeypatch: pytest.MonkeyPatch, ha_app: Application
) -> None:
    """Even if a slow run finishes after demotion the guard in
    ``_safe_group_run`` prevents a stale write from landing in the
    shared cache. Belt and braces alongside the cancellation."""
    ha_app._is_leader = True

    async def slow_then_demote(_group_name: str) -> Any:
        from promanomaly.runner import GroupRunResult

        # Simulate a run that completed (succeeded=True) right as we
        # lost leadership.
        ha_app._is_leader = False
        return GroupRunResult(group="g1", samples=[], succeeded=True, series_count=0)

    monkeypatch.setattr(ha_app._runner, "run_group", slow_then_demote)

    # Pre-seed the store + cache with the previous leader's snapshot
    # so we can prove the demoted run did NOT overwrite it.
    seed = GroupSnapshot(
        group="g1",
        timestamp=1.0,
        samples=[Sample(metric="anomaly_score", labels=(("group", "g1"),), value=42.0)],
    )
    ha_app._store.write(seed)
    assert ha_app._ha is not None
    ha_app._ha.snapshot_cache.publish(seed)

    await ha_app._safe_group_run("g1")

    # The store DID get overwritten by run_group's local write (we
    # can't undo that from outside the runner), but the *Redis*
    # snapshot publish is the cross-pod hazard and that one is gated
    # by the leader check. So the shared cache still carries the
    # original snapshot.
    loaded = ha_app._ha.snapshot_cache.load_all()
    assert len(loaded) == 1
    assert loaded[0].samples[0].value == 42.0
