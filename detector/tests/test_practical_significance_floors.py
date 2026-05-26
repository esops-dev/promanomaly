"""Practical-significance floors gate ``anomaly_outside_threshold``.

A purely statistical detector will (correctly) flag a 0.01 -> 0.012 move
on a sparse error counter as significant; for many operators that is
operational noise. ``defaults.min_abs_delta`` and
``defaults.min_relative_delta`` (with per-query overrides) suppress the
alerting bit when the absolute or relative move is too small, while
leaving ``anomaly_score`` itself untouched so the statistical signal
still appears on dashboards.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pandas as pd
import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.detectors import registry
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series


class _ScriptedDetector:
    """Detector double that returns a fixed (score, baseline) per run."""

    name: ClassVar[str] = "Scripted"
    description: ClassVar[str] = "fixed-output test double"
    defaults: ClassVar[dict[str, Any]] = {}
    # Set per test before invocation.
    score: ClassVar[float] = 0.0
    baseline: ClassVar[float] = 0.0

    def fit_score(self, df: pd.DataFrame, freq: str, params: dict[str, Any]) -> pd.DataFrame:
        del freq, params
        return pd.DataFrame(
            [
                {
                    "timestamp": float(df["timestamp"].iloc[-1]),
                    "score": float(_ScriptedDetector.score),
                    "baseline": float(_ScriptedDetector.baseline),
                    "is_outside": False,
                }
            ]
        )


@pytest.fixture
def scripted() -> Any:
    registry.register(_ScriptedDetector)  # type: ignore[arg-type]
    try:
        yield _ScriptedDetector
    finally:
        registry.unregister("Scripted")
        # Reset class-level scratch so leakage across tests doesn't bite.
        _ScriptedDetector.score = 0.0
        _ScriptedDetector.baseline = 0.0


def _series(latest_y: float) -> pd.DataFrame:
    # 40 samples of constant value then the latest sample overridden so
    # ``min_points`` (30) is satisfied and the runner runs the detector.
    timestamps = [1_700_000_000.0 + i for i in range(40)]
    values = [latest_y] * 40
    return pd.DataFrame({"timestamp": timestamps, "y": values})


def _config(
    *,
    defaults_overrides: dict[str, Any] | None = None,
    query_overrides: dict[str, Any] | None = None,
) -> Config:
    defaults: dict[str, Any] = {
        "window": "15m",
        "step": "15s",
        "min_points": 30,
        "warmup_policy": "emit_warming_up",
        "emit_baseline_stability": False,
        "alert_thresholds": {"score": 3.0},
    }
    defaults.update(defaults_overrides or {})
    query: dict[str, Any] = {
        "id": "m",
        "promql": "up",
        "detectors": [{"name": "Scripted"}],
    }
    query.update(query_overrides or {})
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": defaults,
        "groups": [{"name": "g1", "queries": [query]}],
    }
    return Config.model_validate(raw)


def _runner(cfg: Config, stub: StubSource) -> Runner:
    return Runner(cfg, stub, SnapshotStore(), OperationalMetrics())


@pytest.mark.asyncio
async def test_no_floor_means_threshold_fires_when_score_exceeds(
    stub_source: StubSource,
    scripted: Any,
) -> None:
    scripted.score = 5.0
    scripted.baseline = 0.0
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, _series(0.012))]))

    cfg = _config()
    runner = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    outside = next(s for s in result.samples if s.metric == "anomaly_outside_threshold")
    assert outside.value == 1.0


@pytest.mark.asyncio
async def test_min_abs_delta_blocks_statistically_small_move(
    stub_source: StubSource,
    scripted: Any,
) -> None:
    """High statistical score but delta below the abs floor -> no fire,
    but the raw score is still emitted unchanged."""
    scripted.score = 9.0
    scripted.baseline = 0.01
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, _series(0.012))]))

    cfg = _config(defaults_overrides={"min_abs_delta": 1.0})
    runner = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    outside = next(s for s in result.samples if s.metric == "anomaly_outside_threshold")
    score = next(s for s in result.samples if s.metric == "anomaly_score")
    assert outside.value == 0.0
    assert score.value == pytest.approx(9.0)


@pytest.mark.asyncio
async def test_min_abs_delta_allows_large_move(
    stub_source: StubSource,
    scripted: Any,
) -> None:
    scripted.score = 9.0
    scripted.baseline = 0.0
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, _series(50.0))]))

    cfg = _config(defaults_overrides={"min_abs_delta": 1.0})
    runner = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    outside = next(s for s in result.samples if s.metric == "anomaly_outside_threshold")
    assert outside.value == 1.0


@pytest.mark.asyncio
async def test_min_relative_delta_blocks_proportional_noise(
    stub_source: StubSource,
    scripted: Any,
) -> None:
    # 5% relative floor; the move is 2% of baseline -> blocked.
    scripted.score = 9.0
    scripted.baseline = 100.0
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, _series(102.0))]))

    cfg = _config(defaults_overrides={"min_relative_delta": 0.05})
    runner = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    outside = next(s for s in result.samples if s.metric == "anomaly_outside_threshold")
    assert outside.value == 0.0


@pytest.mark.asyncio
async def test_min_relative_delta_allows_large_proportional_move(
    stub_source: StubSource,
    scripted: Any,
) -> None:
    scripted.score = 9.0
    scripted.baseline = 100.0
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, _series(120.0))]))

    cfg = _config(defaults_overrides={"min_relative_delta": 0.05})
    runner = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    outside = next(s for s in result.samples if s.metric == "anomaly_outside_threshold")
    assert outside.value == 1.0


@pytest.mark.asyncio
async def test_both_floors_must_pass_when_both_set(
    stub_source: StubSource,
    scripted: Any,
) -> None:
    # Abs floor 1.0 passes (delta=2); relative floor 0.5 fails (delta is
    # only 2% of baseline=100). Both floors set -> alert blocked.
    scripted.score = 9.0
    scripted.baseline = 100.0
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, _series(102.0))]))

    cfg = _config(defaults_overrides={"min_abs_delta": 1.0, "min_relative_delta": 0.5})
    runner = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    outside = next(s for s in result.samples if s.metric == "anomaly_outside_threshold")
    assert outside.value == 0.0


@pytest.mark.asyncio
async def test_min_relative_delta_with_zero_baseline_degenerates_to_absolute(
    stub_source: StubSource,
    scripted: Any,
) -> None:
    """Documented behaviour: when baseline=0, min_relative_delta
    behaves as an absolute-units floor (denominator is treated as 1.0)
    so sparse-counter warm-up doesn't trip a divide-by-zero or always-
    pass surprise."""
    scripted.score = 9.0
    scripted.baseline = 0.0
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, _series(0.02))]))

    # Move = 0.02, relative floor degenerates to "needs abs >= 0.05" -> blocked.
    cfg = _config(defaults_overrides={"min_relative_delta": 0.05})
    runner = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    outside = next(s for s in result.samples if s.metric == "anomaly_outside_threshold")
    assert outside.value == 0.0

    # Same config, larger move -> passes.
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, _series(0.10))]))
    cfg2 = _config(defaults_overrides={"min_relative_delta": 0.05})
    runner2 = _runner(cfg2, stub_source)
    result2 = await runner2.run_group("g1")
    outside2 = next(s for s in result2.samples if s.metric == "anomaly_outside_threshold")
    assert outside2.value == 1.0


@pytest.mark.asyncio
async def test_query_floor_overrides_defaults(
    stub_source: StubSource,
    scripted: Any,
) -> None:
    """Per-query overrides take precedence over group defaults."""
    scripted.score = 9.0
    scripted.baseline = 0.01
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, _series(0.012))]))

    # Group default would block; the per-query override relaxes to 0.
    cfg = _config(
        defaults_overrides={"min_abs_delta": 1.0},
        query_overrides={"min_abs_delta": 0.0},
    )
    runner = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    outside = next(s for s in result.samples if s.metric == "anomaly_outside_threshold")
    assert outside.value == 1.0
