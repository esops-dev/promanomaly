"""Tests for the three ``safety.on_source_failure`` policies.

``serve_stale`` is exercised in test_runner.py — the cases here cover
``drop_scores`` (failed queries are dropped from the next snapshot
while other queries still write) and ``fail_ready`` (sustained source
failures flip the group's readiness off after the configured grace).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.source import SourceQueryError
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline


def _config(
    *,
    on_source_failure: str = "serve_stale",
    fail_ready_after: int = 3,
    queries: list[dict[str, Any]] | None = None,
) -> Config:
    queries = queries or [{"id": "m1", "promql": "up{job='ok'}", "detectors": [{"name": "MAD"}]}]
    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "safety": {
            "on_source_failure": on_source_failure,
            "fail_ready_after": fail_ready_after,
        },
        "defaults": {
            "window": "5m",
            "step": "15s",
            "min_points": 5,
            "warmup_policy": "emit_warming_up",
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [{"name": "g1", "queries": queries}],
    }
    return Config.model_validate(raw)


def _components(cfg: Config, stub: StubSource) -> tuple[Runner, SnapshotStore, OperationalMetrics]:
    store = SnapshotStore()
    ops = OperationalMetrics()
    return Runner(cfg, stub, store, ops), store, ops


def _alternating_responder(
    good_payload: Callable[[], StubResponse],
    bad_payload: Callable[[], StubResponse],
    pattern: list[str],
) -> Callable[[str], StubResponse]:
    """Return a responder that walks ``pattern`` (each entry 'ok' or 'fail')."""
    state = {"i": 0}

    def respond(_: str) -> StubResponse:
        idx = state["i"] % len(pattern)
        state["i"] += 1
        return good_payload() if pattern[idx] == "ok" else bad_payload()

    return respond


# ---------------------------------------------------------------------------
# drop_scores
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drop_scores_keeps_other_queries_in_snapshot(stub_source: StubSource) -> None:
    """A failing query should not abort the run under drop_scores."""

    def respond(promql: str) -> StubResponse:
        if "bad" in promql:
            return StubResponse(error=SourceQueryError("server_error", "boom"))
        return StubResponse(series=[make_series({"instance": "h"}, clean_baseline(n=120))])

    stub_source.respond(respond)
    cfg = _config(
        on_source_failure="drop_scores",
        queries=[
            {"id": "m_ok", "promql": "up{job='ok'}", "detectors": [{"name": "MAD"}]},
            {"id": "m_bad", "promql": "bad_query", "detectors": [{"name": "MAD"}]},
        ],
    )
    runner, store, ops = _components(cfg, stub_source)

    result = await runner.run_group("g1")

    # The run as a whole succeeds even though one query failed.
    assert result.succeeded
    ids = {dict(s.labels).get("id") for s in result.samples}
    assert ids == {"m_ok"}, ids
    # The source-failure counter still bumped.
    counter = ops.registry.get_sample_value(
        "anomaly_source_failure_total", {"group": "g1", "reason": "server_error"}
    )
    assert counter == pytest.approx(1.0)
    # Snapshot has been overwritten with the fresh, partial data.
    snap = store.get("g1")
    assert snap is not None
    assert {dict(s.labels).get("id") for s in snap.samples} == {"m_ok"}


@pytest.mark.asyncio
async def test_drop_scores_all_queries_failing_preserves_prior_snapshot(
    stub_source: StubSource,
) -> None:
    """All queries failing under drop_scores must NOT overwrite the previous snapshot.

    Otherwise a transient outage degrades drop_scores to "lose the last
    good data", which is worse than serve_stale. The all-fail edge case
    is treated as a whole-group source failure: counter bumps, streak
    bumps, snapshot stays.
    """
    pattern = ["ok", "fail"]
    stub_source.respond(
        _alternating_responder(
            good_payload=lambda: StubResponse(
                series=[make_series({"instance": "h"}, clean_baseline(n=120))]
            ),
            bad_payload=lambda: StubResponse(error=SourceQueryError("timeout", "slow")),
            pattern=pattern,
        )
    )

    cfg = _config(on_source_failure="drop_scores")
    runner, store, ops = _components(cfg, stub_source)

    first = await runner.run_group("g1")
    assert first.succeeded
    pre_snapshot = store.get("g1")
    assert pre_snapshot is not None
    assert any(s.metric == "anomaly_score" for s in pre_snapshot.samples)

    second = await runner.run_group("g1")
    # The all-fail run is reported as failed, not a successful empty run.
    assert not second.succeeded
    # The previous snapshot is unchanged.
    assert store.get("g1") is pre_snapshot
    # The streak bumped — operators can observe instability via the gauge.
    assert store.source_failure_streak("g1") == 1
    counter = ops.registry.get_sample_value(
        "anomaly_source_failure_total", {"group": "g1", "reason": "timeout"}
    )
    assert counter == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_drop_scores_partial_failure_writes_partial_snapshot(
    stub_source: StubSource,
) -> None:
    """At least one query succeeding under drop_scores still produces a fresh snapshot."""

    def respond(promql: str) -> StubResponse:
        if "bad" in promql:
            return StubResponse(error=SourceQueryError("timeout", "slow"))
        return StubResponse(series=[make_series({"instance": "h"}, clean_baseline(n=120))])

    stub_source.respond(respond)

    cfg = _config(
        on_source_failure="drop_scores",
        queries=[
            {"id": "m_ok", "promql": "up", "detectors": [{"name": "MAD"}]},
            {"id": "m_bad", "promql": "bad", "detectors": [{"name": "MAD"}]},
        ],
    )
    runner, store, _ops = _components(cfg, stub_source)

    result = await runner.run_group("g1")
    assert result.succeeded
    snap = store.get("g1")
    assert snap is not None
    assert {dict(s.labels).get("id") for s in snap.samples} == {"m_ok"}
    # Partial success resets the streak — drop_scores degraded but
    # didn't fail wholesale.
    assert store.source_failure_streak("g1") == 0


# ---------------------------------------------------------------------------
# fail_ready
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fail_ready_single_failure_keeps_group_ready(stub_source: StubSource) -> None:
    """One transient source failure must not trip readiness."""
    pattern = ["ok", "fail"]
    stub_source.respond(
        _alternating_responder(
            good_payload=lambda: StubResponse(
                series=[make_series({"instance": "h"}, clean_baseline(n=120))]
            ),
            bad_payload=lambda: StubResponse(error=SourceQueryError("timeout", "blip")),
            pattern=pattern,
        )
    )

    cfg = _config(on_source_failure="fail_ready", fail_ready_after=3)
    runner, store, _ops = _components(cfg, stub_source)

    await runner.run_group("g1")  # ok -> ready=True
    assert store.group_ready("g1")
    await runner.run_group("g1")  # fail -> streak=1, still ready
    assert store.group_ready("g1")


@pytest.mark.asyncio
async def test_fail_ready_trips_after_threshold_consecutive_failures(
    stub_source: StubSource,
) -> None:
    """After ``fail_ready_after`` failures in a row, readiness flips off."""
    stub_source.respond(lambda _: StubResponse(error=SourceQueryError("timeout", "slow")))

    cfg = _config(on_source_failure="fail_ready", fail_ready_after=2)
    runner, store, _ops = _components(cfg, stub_source)

    # First failure — streak=1, still ready (would only flip on >= threshold)
    # but the group hasn't been ready yet; the gauge is False from the start.
    await runner.run_group("g1")
    assert not store.group_ready("g1")  # never had a successful run
    # Same after second failure — but now the streak has tripped explicitly.
    await runner.run_group("g1")
    assert not store.group_ready("g1")
    assert store.source_failure_streak("g1") == 2


@pytest.mark.asyncio
async def test_fail_ready_recovers_after_successful_run(stub_source: StubSource) -> None:
    """A single success after a tripped streak restores readiness and resets the streak."""
    pattern = ["fail", "fail", "ok"]
    stub_source.respond(
        _alternating_responder(
            good_payload=lambda: StubResponse(
                series=[make_series({"instance": "h"}, clean_baseline(n=120))]
            ),
            bad_payload=lambda: StubResponse(error=SourceQueryError("server_error", "5xx")),
            pattern=pattern,
        )
    )

    cfg = _config(on_source_failure="fail_ready", fail_ready_after=2)
    runner, store, _ops = _components(cfg, stub_source)

    await runner.run_group("g1")  # fail (streak=1)
    await runner.run_group("g1")  # fail (streak=2 -> trips)
    assert not store.group_ready("g1")
    assert store.source_failure_streak("g1") == 2

    await runner.run_group("g1")  # ok -> ready=True, streak reset
    assert store.group_ready("g1")
    assert store.source_failure_streak("g1") == 0


@pytest.mark.asyncio
async def test_fail_ready_preserves_last_good_snapshot(stub_source: StubSource) -> None:
    """fail_ready behaves like serve_stale for snapshot retention."""
    pattern = ["ok", "fail", "fail"]
    stub_source.respond(
        _alternating_responder(
            good_payload=lambda: StubResponse(
                series=[make_series({"instance": "h"}, clean_baseline(n=120))]
            ),
            bad_payload=lambda: StubResponse(error=SourceQueryError("timeout", "slow")),
            pattern=pattern,
        )
    )

    cfg = _config(on_source_failure="fail_ready", fail_ready_after=2)
    runner, store, _ops = _components(cfg, stub_source)

    await runner.run_group("g1")  # ok -> snapshot written
    await runner.run_group("g1")  # fail -> snapshot preserved
    await runner.run_group("g1")  # fail -> snapshot preserved, ready off

    snap = store.get("g1")
    assert snap is not None
    assert any(s.metric == "anomaly_score" for s in snap.samples)
    assert not store.group_ready("g1")


@pytest.mark.asyncio
async def test_serve_stale_tracks_streak_but_does_not_gate_readiness(
    stub_source: StubSource,
) -> None:
    """Streak gauge is policy-independent; only fail_ready gates readiness on it."""
    stub_source.respond(lambda _: StubResponse(error=SourceQueryError("timeout", "slow")))

    # fail_ready_after=1 would trip immediately if the policy were fail_ready;
    # under serve_stale it is irrelevant.
    cfg = _config(on_source_failure="serve_stale", fail_ready_after=1)
    runner, store, ops = _components(cfg, stub_source)

    await runner.run_group("g1")
    await runner.run_group("g1")
    # Streak bumps so operators can observe instability regardless of policy.
    assert store.source_failure_streak("g1") == 2
    # But readiness is governed only by whether any successful run has
    # happened — never a transient TSDB blip under serve_stale.
    assert not store.group_ready("g1")
    # The gauge is published on the operational registry.
    gauge = ops.registry.get_sample_value("anomaly_source_failure_streak", {"group": "g1"})
    assert gauge == pytest.approx(2.0)
