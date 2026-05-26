"""Tests for the ``expect: true`` static absence-detection flag."""

from __future__ import annotations

from typing import Any

import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline


def _config_with_expect(grace: int = 2) -> Config:
    return Config.model_validate(
        {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://stub"},
            "defaults": {
                "window": "5m",
                "step": "15s",
                "min_points": 5,
                "warmup_policy": "emit_warming_up",
                "alert_thresholds": {"score": 3.0},
            },
            "groups": [
                {
                    "name": "g1",
                    "queries": [
                        {
                            "id": "heartbeat",
                            "promql": "up{job='hb'}",
                            "expect": True,
                            "expect_grace_runs": grace,
                            "detectors": [{"name": "MAD"}],
                        }
                    ],
                }
            ],
        }
    )


def _components(cfg: Config, stub: StubSource) -> tuple[Runner, SnapshotStore, OperationalMetrics]:
    store = SnapshotStore()
    ops = OperationalMetrics()
    return Runner(cfg, stub, store, ops), store, ops


@pytest.mark.asyncio
async def test_signal_missing_after_grace_runs(stub_source: StubSource) -> None:
    cfg = _config_with_expect(grace=2)
    runner, _store, ops = _components(cfg, stub_source)

    stub_source.respond(lambda _: StubResponse(series=[]))

    first = await runner.run_group("g1")
    assert first.succeeded
    # After 1 miss (< grace), nothing emitted.
    assert all(s.metric != "anomaly_signal_missing" for s in first.samples)

    second = await runner.run_group("g1")
    assert any(s.metric == "anomaly_signal_missing" for s in second.samples)
    missing = next(s for s in second.samples if s.metric == "anomaly_signal_missing")
    assert dict(missing.labels) == {"id": "heartbeat", "group": "g1"}
    assert missing.value == 1.0

    # signal_missing_total counter bumped exactly once (on the
    # transition into the missing state).
    counter = ops.registry.get_sample_value(
        "anomaly_signal_missing_total",
        {"group": "g1", "reason": "missing_grace_exhausted"},
    )
    assert counter == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_signal_missing_clears_when_series_returns(stub_source: StubSource) -> None:
    cfg = _config_with_expect(grace=1)
    runner, _store, _ops = _components(cfg, stub_source)

    state: dict[str, Any] = {"calls": 0}

    def respond(_: str) -> StubResponse:
        state["calls"] += 1
        if state["calls"] == 1:
            return StubResponse(series=[])
        return StubResponse(series=[make_series({}, clean_baseline(n=120))])

    stub_source.respond(respond)

    first = await runner.run_group("g1")
    assert any(s.metric == "anomaly_signal_missing" for s in first.samples)

    second = await runner.run_group("g1")
    # The query now returns results, so signal_missing is *not* emitted.
    assert all(s.metric != "anomaly_signal_missing" for s in second.samples)
