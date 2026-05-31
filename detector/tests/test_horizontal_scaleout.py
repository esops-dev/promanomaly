"""Horizontal scale-out and resilience.

Covers three orthogonal capabilities:

* Active/active sharding via consistent (rendezvous) hashing.
* Per-group refresh intervals.
* Priority-aware scheduling and load shedding.
"""

from __future__ import annotations

import asyncio
import types
from pathlib import Path
from typing import Any

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pydantic import ValidationError

from promanomaly.cli._cost import estimate_cost
from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.exporter import OperationalMetrics
from promanomaly.main import Application
from promanomaly.runner import Runner
from promanomaly.scheduling import LoadShedder
from promanomaly.sharding import ShardAssignment, resolve_shard_index
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline


def _metric_value(ops: OperationalMetrics, name: str, labels: dict[str, str]) -> float:
    value = ops.registry.get_sample_value(name, labels)
    return float(value) if value is not None else 0.0


# ── Config: scale-out, per-group refresh, max_concurrent_groups ──────


def _base_config(**overrides: Any) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://vm:8428/"},
        "groups": [
            {
                "name": "g1",
                "queries": [{"id": "metric_a", "promql": "up", "detectors": [{"name": "MAD"}]}],
            }
        ],
    }
    raw.update(overrides)
    return raw


class TestScaleOutConfig:
    def test_defaults_off(self) -> None:
        cfg = Config.model_validate(_base_config())
        assert cfg.scaleOut.enabled is False
        assert cfg.scaleOut.shards == 1
        assert cfg.scaleOut.shard_index is None

    def test_shard_index_must_be_below_shards(self) -> None:
        with pytest.raises(Exception, match=r"shard_index=2 must be < scaleOut.shards=2"):
            Config.model_validate(
                _base_config(scaleOut={"enabled": True, "shards": 2, "shard_index": 2})
            )

    def test_scaleout_and_ha_mutually_exclusive(self) -> None:
        with pytest.raises(Exception, match=r"mutually exclusive"):
            Config.model_validate(
                _base_config(
                    scaleOut={"enabled": True, "shards": 2},
                    highAvailability={"enabled": True},
                    safety={"redis": {"url": "redis://localhost:6379/0"}},
                )
            )

    def test_scaleout_enabled_validates(self) -> None:
        cfg = Config.model_validate(
            _base_config(scaleOut={"enabled": True, "shards": 4, "shard_index": 1})
        )
        assert cfg.scaleOut.enabled
        assert cfg.scaleOut.shards == 4
        assert cfg.scaleOut.shard_index == 1


class TestPerGroupRefresh:
    def test_group_override_takes_precedence(self) -> None:
        raw = _base_config(server={"refresh_interval": "1m"})
        raw["groups"][0]["refresh_interval"] = "30s"
        cfg = Config.model_validate(raw)
        assert cfg.group_refresh_seconds(cfg.groups[0]) == 30.0

    def test_falls_back_to_server_default(self) -> None:
        cfg = Config.model_validate(_base_config(server={"refresh_interval": "90s"}))
        assert cfg.groups[0].refresh_interval_override_seconds is None
        assert cfg.group_refresh_seconds(cfg.groups[0]) == 90.0

    def test_aggressive_refresh_groups_flags_per_group_override(self) -> None:
        # window=1h → floor = 600s. A 30s per-group override is aggressive.
        raw = _base_config(
            server={"refresh_interval": "10m"},
            defaults={"window": "1h"},
        )
        raw["groups"][0]["refresh_interval"] = "30s"
        cfg = Config.model_validate(raw)
        offending = cfg.aggressive_refresh_groups()
        assert offending == [("g1", 30.0)]

    def test_aggressive_refresh_groups_empty_when_within_floor(self) -> None:
        cfg = Config.model_validate(
            _base_config(server={"refresh_interval": "10m"}, defaults={"window": "1h"})
        )
        assert cfg.aggressive_refresh_groups() == []


class TestMaxConcurrentConfig:
    def test_defaults_to_none(self) -> None:
        cfg = Config.model_validate(_base_config())
        assert cfg.safety.max_concurrent_groups is None

    def test_accepts_positive_int(self) -> None:
        cfg = Config.model_validate(_base_config(safety={"max_concurrent_groups": 2}))
        assert cfg.safety.max_concurrent_groups == 2

    def test_rejects_zero(self) -> None:
        with pytest.raises(ValidationError):
            Config.model_validate(_base_config(safety={"max_concurrent_groups": 0}))


# ── Sharding: rendezvous hashing ─────────────────────────────────────


def _key(instance: str) -> tuple[tuple[str, str], ...]:
    return (("instance", instance),)


class TestShardAssignment:
    def test_single_shard_owns_everything(self) -> None:
        shard = ShardAssignment(index=0, count=1)
        assert all(shard.owns(_key(f"host-{i}")) for i in range(20))

    def test_partition_is_disjoint_and_covers_all(self) -> None:
        keys = [_key(f"host-{i}") for i in range(50)]
        shards = [ShardAssignment(index=i, count=3) for i in range(3)]
        owned: list[set[int]] = []
        for shard in shards:
            owned.append({i for i, k in enumerate(keys) if shard.owns(k)})
        # Every key owned by exactly one shard.
        union: set[int] = set().union(*owned)
        assert union == set(range(50))
        assert sum(len(o) for o in owned) == 50  # disjoint

    def test_ownership_is_deterministic(self) -> None:
        shard_a = ShardAssignment(index=2, count=5)
        shard_b = ShardAssignment(index=2, count=5)
        key = _key("host-xyz")
        assert shard_a.owns(key) == shard_b.owns(key)

    def test_minimal_movement_on_resize(self) -> None:
        # Rendezvous hashing: growing the ring should move far fewer keys
        # than a modulo scheme (which reshuffles almost everything).
        keys = [_key(f"host-{i}") for i in range(300)]

        def owner(count: int, key: tuple[tuple[str, str], ...]) -> int:
            for i in range(count):
                if ShardAssignment(index=i, count=count).owns(key):
                    return i
            raise AssertionError("no owner")

        before = {i: owner(3, k) for i, k in enumerate(keys)}
        after = {i: owner(4, k) for i, k in enumerate(keys)}
        moved = sum(1 for i in before if before[i] != after[i])
        # Ideal HRW reassigns ~1/4 of keys when going 3→4; allow slack.
        assert moved < len(keys) * 0.5

    def test_rejects_out_of_range_index(self) -> None:
        with pytest.raises(ValueError):
            ShardAssignment(index=3, count=3)


class TestResolveShardIndex:
    def test_explicit_index_wins(self) -> None:
        assert resolve_shard_index(2, shards=4) == 2

    def test_reads_statefulset_ordinal_from_pod_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POD_NAME", "promanomaly-2")
        assert resolve_shard_index(None, shards=4) == 2

    def test_defaults_to_zero_without_pod_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("POD_NAME", raising=False)
        assert resolve_shard_index(None, shards=1) == 0

    def test_out_of_range_ordinal_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POD_NAME", "promanomaly-9")
        with pytest.raises(ValueError, match=r"out of range"):
            resolve_shard_index(None, shards=4)


# ── Runner: shard filter scores only owned series ────────────────────


def _runner_config(shards: int = 2, *, max_total_series: int | None = None) -> Config:
    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {"window": "15m", "step": "15s", "min_points": 30},
        "groups": [
            {"name": "g1", "queries": [{"id": "m", "promql": "up", "detectors": [{"name": "MAD"}]}]}
        ],
    }
    if shards:
        raw["scaleOut"] = {"enabled": True, "shards": shards}
    if max_total_series is not None:
        raw["safety"] = {"max_total_series": max_total_series}
    return Config.model_validate(raw)


def _series_set(n: int) -> list[Any]:
    df = clean_baseline(n=120)
    return [make_series({"instance": f"host-{i}"}, df) for i in range(n)]


@pytest.mark.asyncio
async def test_shards_partition_series(stub_source: StubSource) -> None:
    n = 40
    stub_source.respond(lambda _: StubResponse(series=_series_set(n)))
    cfg = _runner_config(shards=2)

    counts = []
    owned_ids: set[str] = set()
    for index in range(2):
        store = SnapshotStore()
        ops = OperationalMetrics()
        runner = Runner(cfg, stub_source, store, ops, shard=ShardAssignment(index, 2))
        result = await runner.run_group("g1")
        assert result.succeeded
        counts.append(result.series_count)
        ids = {
            dict(s.labels).get("instance") for s in result.samples if s.metric == "anomaly_score"
        }
        owned_ids |= {i for i in ids if i is not None}

    # Disjoint partition that together covers every input series.
    assert sum(counts) == n
    assert len(owned_ids) == n


@pytest.mark.asyncio
async def test_no_shard_scores_everything(stub_source: StubSource) -> None:
    stub_source.respond(lambda _: StubResponse(series=_series_set(10)))
    cfg = _runner_config(shards=0)
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops, shard=None)
    result = await runner.run_group("g1")
    assert result.series_count == 10


@pytest.mark.asyncio
async def test_shard_series_count_metric_emitted(stub_source: StubSource) -> None:
    stub_source.respond(lambda _: StubResponse(series=_series_set(30)))
    cfg = _runner_config(shards=2)
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops, shard=ShardAssignment(0, 2))
    result = await runner.run_group("g1")
    gauge = _metric_value(ops, "anomaly_shard_series_count", {"shard": "0"})
    assert gauge == float(result.series_count)


@pytest.mark.asyncio
async def test_max_total_series_is_a_per_shard_cap(stub_source: StubSource) -> None:
    # 60 series across 2 shards (each owns ~30), cap=5 per shard. With a
    # per-shard cap each shard scores exactly 5 and the estate covers 10
    # (cap x shards) — not 5 total, which an estate-wide cap would give.
    stub_source.respond(lambda _: StubResponse(series=_series_set(60)))
    cfg = _runner_config(shards=2, max_total_series=5)
    estate = 0
    for index in range(2):
        runner = Runner(
            cfg, stub_source, SnapshotStore(), OperationalMetrics(), shard=ShardAssignment(index, 2)
        )
        result = await runner.run_group("g1")
        assert result.series_count == 5  # each shard capped at its own budget
        estate += result.series_count
    assert estate == 10  # cap (5) x shards (2)


@pytest.mark.asyncio
async def test_density_suppressed_under_sharding(stub_source: StubSource) -> None:
    stub_source.respond(lambda _: StubResponse(series=_series_set(20)))
    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {"window": "15m", "step": "15s", "min_points": 30, "density_by": ["instance"]},
        "scaleOut": {"enabled": True, "shards": 2},
        "groups": [
            {"name": "g1", "queries": [{"id": "m", "promql": "up", "detectors": [{"name": "MAD"}]}]}
        ],
    }
    cfg = Config.model_validate(raw)
    runner = Runner(
        cfg, stub_source, SnapshotStore(), OperationalMetrics(), shard=ShardAssignment(0, 2)
    )
    result = await runner.run_group("g1")
    assert not [s for s in result.samples if s.metric == "anomaly_density"]
    # …but the unsharded runner still emits it.
    cfg_plain = Config.model_validate({**raw, "scaleOut": {"enabled": False}})
    plain = Runner(cfg_plain, stub_source, SnapshotStore(), OperationalMetrics(), shard=None)
    plain_result = await plain.run_group("g1")
    assert [s for s in plain_result.samples if s.metric == "anomaly_density"]


@pytest.mark.asyncio
async def test_expect_absence_emitted_by_exactly_one_shard(stub_source: StubSource) -> None:
    # An expected query that returns nothing should emit a single
    # anomaly_signal_missing across the shard set, not one per shard.
    stub_source.respond(lambda _: StubResponse(series=[]))
    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {"window": "15m", "step": "15s", "min_points": 30},
        "scaleOut": {"enabled": True, "shards": 3},
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "heartbeat",
                        "promql": "up",
                        "expect": True,
                        "expect_grace_runs": 1,
                        "detectors": [{"name": "MAD"}],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    emitting = 0
    for index in range(3):
        runner = Runner(
            cfg, stub_source, SnapshotStore(), OperationalMetrics(), shard=ShardAssignment(index, 3)
        )
        result = await runner.run_group("g1")
        if [s for s in result.samples if s.metric == "anomaly_signal_missing"]:
            emitting += 1
    assert emitting == 1  # exactly one shard owns the group's whole-query absence


def test_shard_owns_group_has_exactly_one_owner() -> None:
    cfg = _runner_config(shards=4)
    owners = [
        Runner(
            cfg, StubSource(), SnapshotStore(), OperationalMetrics(), shard=ShardAssignment(i, 4)
        )._shard_owns_group("g1")
        for i in range(4)
    ]
    assert owners.count(True) == 1


# ── Scheduling: priority-aware load shedding ─────────────────────────


class TestLoadShedder:
    @pytest.mark.asyncio
    async def test_uncapped_is_passthrough(self) -> None:
        shedder = LoadShedder(None)
        assert await shedder.acquire(priority=1, deadline_seconds=0.0)
        assert shedder.running == 1
        shedder.release()
        assert shedder.running == 0

    @pytest.mark.asyncio
    async def test_admits_up_to_cap(self) -> None:
        shedder = LoadShedder(2)
        assert await shedder.acquire(priority=1, deadline_seconds=1.0)
        assert await shedder.acquire(priority=1, deadline_seconds=1.0)
        assert shedder.running == 2

    @pytest.mark.asyncio
    async def test_sheds_when_saturated(self) -> None:
        shedder = LoadShedder(1)
        assert await shedder.acquire(priority=1, deadline_seconds=1.0)
        shed = await shedder.acquire(priority=1, deadline_seconds=0.05)
        assert shed is False
        assert shedder.running == 1

    @pytest.mark.asyncio
    async def test_cancellation_propagates_and_frees_no_slot(self) -> None:
        # A waiter cancelled (leadership loss / shutdown) must raise
        # CancelledError, not silently resolve as a shed, and must not
        # leak a concurrency slot.
        shedder = LoadShedder(1)
        assert await shedder.acquire(priority=1, deadline_seconds=5.0)

        task = asyncio.create_task(shedder.acquire(priority=1, deadline_seconds=5.0))
        while len(shedder._waiters) < 1:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # The original holder still holds exactly one slot; the cancelled
        # waiter left nothing behind.
        assert shedder.running == 1
        assert shedder._waiters == []

    @pytest.mark.asyncio
    async def test_higher_priority_wakes_first(self) -> None:
        shedder = LoadShedder(1)
        assert await shedder.acquire(priority=1, deadline_seconds=5.0)
        order: list[int] = []

        async def waiter(priority: int) -> None:
            admitted = await shedder.acquire(priority=priority, deadline_seconds=5.0)
            order.append(priority)
            if admitted:
                shedder.release()

        low = asyncio.create_task(waiter(1))
        high = asyncio.create_task(waiter(10))
        while len(shedder._waiters) < 2:
            await asyncio.sleep(0)
        shedder.release()  # frees one slot; highest priority served first
        await asyncio.gather(low, high)
        assert order[0] == 10

    @pytest.mark.asyncio
    async def test_raising_cap_admits_waiter(self) -> None:
        shedder = LoadShedder(1)
        assert await shedder.acquire(priority=1, deadline_seconds=5.0)
        admitted: list[bool] = []

        async def waiter() -> None:
            admitted.append(await shedder.acquire(priority=1, deadline_seconds=5.0))

        task = asyncio.create_task(waiter())
        while len(shedder._waiters) < 1:
            await asyncio.sleep(0)
        shedder.set_max_concurrent(2)  # now the queued waiter fits
        await task
        assert admitted == [True]


# ── estimate-cost accounts for per-group cadence ─────────────────────


def test_estimate_cost_uses_per_group_refresh() -> None:
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://vm:8428/"},
        "server": {"refresh_interval": "60s"},
        "groups": [
            {
                "name": "fast",
                "refresh_interval": "30s",
                "queries": [{"id": "a", "promql": "up", "detectors": [{"name": "MAD"}]}],
            },
            {
                "name": "slow",
                "refresh_interval": "300s",
                "queries": [{"id": "b", "promql": "up", "detectors": [{"name": "MAD"}]}],
            },
        ],
    }
    cfg = Config.model_validate(raw)
    estimates = {g.group: g for g in estimate_cost(cfg)}
    # One short query per refresh each, but the 30s group issues 10x the
    # per-minute load of the 5m group.
    assert estimates["fast"].refresh_interval_seconds == 30.0
    assert estimates["slow"].refresh_interval_seconds == 300.0
    assert estimates["fast"].short_queries_per_minute == 2.0
    assert estimates["slow"].short_queries_per_minute == 0.2
    # Faster cadence ⇒ more steady-state CPU for the same series count.
    assert estimates["fast"].cpu_cores > estimates["slow"].cpu_cores


# ── Application scheduler wiring ─────────────────────────────────────


def _app(cfg: Config) -> Application:
    return Application(cfg, Path("dummy.yaml"))


def _job_intervals(app: Application) -> dict[str, float]:
    scheduler = AsyncIOScheduler()
    app._reschedule_jobs(scheduler)
    intervals: dict[str, float] = {}
    for job in scheduler.get_jobs():
        intervals[job.id] = job.trigger.interval.total_seconds()
    return intervals


def test_scheduler_uses_per_group_refresh_interval() -> None:
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"refresh_interval": "60s"},
        "groups": [
            {
                "name": "fast",
                "refresh_interval": "30s",
                "queries": [{"id": "a", "promql": "up", "detectors": [{"name": "MAD"}]}],
            },
            {
                "name": "slow",
                "queries": [{"id": "b", "promql": "up", "detectors": [{"name": "MAD"}]}],
            },
        ],
    }
    intervals = _job_intervals(_app(Config.model_validate(raw)))
    assert intervals["group:fast"] == 30.0
    assert intervals["group:slow"] == 60.0  # inherits server default


def test_overrun_listener_increments_counter() -> None:
    app = _app(Config.model_validate(_base_config()))
    event = types.SimpleNamespace(job_id="group:g1")
    app._on_job_max_instances(event)
    assert (
        _metric_value(app._ops, "anomaly_group_skipped_total", {"group": "g1", "reason": "overrun"})
        == 1.0
    )


def test_overrun_listener_ignores_non_group_jobs() -> None:
    app = _app(Config.model_validate(_base_config()))
    app._on_job_max_instances(types.SimpleNamespace(job_id="selftest"))
    samples = list(app._ops.group_skipped_total.collect()[0].samples)
    assert samples == []  # nothing counted for a non-group job


@pytest.mark.asyncio
async def test_admit_and_run_sheds_under_backpressure() -> None:
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "safety": {"max_concurrent_groups": 1},
        "groups": [
            {
                "name": "low",
                "priority": 1,
                "refresh_interval": "0.1s",
                "queries": [{"id": "a", "promql": "up", "detectors": [{"name": "MAD"}]}],
            }
        ],
    }
    app = _app(Config.model_validate(raw))
    # Saturate the single slot so the next admission must shed.
    assert await app._load_shedder.acquire(priority=5, deadline_seconds=10.0)
    await app._admit_and_run("low")
    assert (
        _metric_value(
            app._ops, "anomaly_group_skipped_total", {"group": "low", "reason": "backpressure"}
        )
        == 1.0
    )


def test_reschedule_jobs_does_not_register_listeners() -> None:
    # Regression: the overrun listener must be registered once (in
    # startup), never inside _reschedule_jobs — otherwise every reload
    # stacks a duplicate listener and multiplies the overrun counter.
    app = _app(Config.model_validate(_base_config()))
    scheduler = AsyncIOScheduler()
    before = len(scheduler._listeners)
    app._reschedule_jobs(scheduler)
    app._reschedule_jobs(scheduler)
    assert len(scheduler._listeners) == before  # reschedule is listener-neutral


# ── Regression: scaleOut reload + group-removal cleanup ──────────────


def _yaml_config(tmp_path: Path, shards: int) -> Path:
    import yaml

    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "scaleOut": {"enabled": True, "shards": shards},
        "groups": [
            {"name": "g1", "queries": [{"id": "m", "promql": "up", "detectors": [{"name": "MAD"}]}]}
        ],
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


@pytest.mark.asyncio
async def test_reload_rejects_scaleout_change(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from promanomaly.config import load_config

    monkeypatch.delenv("POD_NAME", raising=False)  # shard index resolves to 0
    monkeypatch.setattr("promanomaly.main.PromQLSource", lambda **_k: StubSource())
    path = _yaml_config(tmp_path, shards=2)
    app = Application(load_config(path), path)

    # Operator edits shards on disk, then triggers a hot reload.
    path.write_text(path.read_text().replace("shards: 2", "shards: 3"))
    ok, msg = await app.reload()

    assert ok is False
    assert "scaleOut" in msg and "restart" in msg
    assert app._config.scaleOut.shards == 2  # previous config stays active


def test_replace_config_drops_removed_group_series_count(stub_source: StubSource) -> None:
    def _two_group_cfg(names: list[str]) -> Config:
        return Config.model_validate(
            {
                "apiVersion": CURRENT_API_VERSION,
                "datasource": {"url": "http://stub"},
                "defaults": {"window": "15m", "step": "15s", "min_points": 30},
                "groups": [
                    {
                        "name": n,
                        "queries": [{"id": "m", "promql": "up", "detectors": [{"name": "MAD"}]}],
                    }
                    for n in names
                ],
            }
        )

    runner = Runner(
        _two_group_cfg(["g1", "gone"]), stub_source, SnapshotStore(), OperationalMetrics()
    )
    runner._last_series_count["g1"] = 100
    runner._last_series_count["gone"] = 50
    # Reload drops 'gone'; its sticky count must go too so it can't keep
    # shrinking the cross-group budget for surviving groups after reload.
    runner.replace_config(_two_group_cfg(["g1"]))
    assert "gone" not in runner._last_series_count
    assert runner._last_series_count.get("g1") == 100


class TestLoadShedderReload:
    @pytest.mark.asyncio
    async def test_uncapping_releases_queued_waiters(self) -> None:
        # set_max_concurrent(None) means "admit everyone" — a queued
        # waiter must be released immediately, not left to shed against a
        # cap that no longer exists.
        shedder = LoadShedder(1)
        assert await shedder.acquire(priority=1, deadline_seconds=5.0)
        admitted: list[bool] = []

        async def waiter() -> None:
            admitted.append(await shedder.acquire(priority=1, deadline_seconds=5.0))

        task = asyncio.create_task(waiter())
        while len(shedder._waiters) < 1:
            await asyncio.sleep(0)
        shedder.set_max_concurrent(None)  # uncap → drain the queue now
        await task
        assert admitted == [True]
        assert shedder._waiters == []
