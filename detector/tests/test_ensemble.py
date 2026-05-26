"""Ensemble fusion tests.

The runner emits ``anomaly_composite_score`` and
``anomaly_composite_outside_threshold`` only when the group declares an
``ensemble:`` block. Two fusion methods are supported and only two:
``max`` (logical OR across detectors) and ``voting`` (at least
``min_detectors`` agree). Both keep the composite score as the
maximum raw per-detector score so the gauge is comparable across modes.
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
from tests.fixtures import clean_baseline, point_spike


def _config(
    *,
    ensemble: dict[str, Any] | None,
    detectors: list[dict[str, Any]],
) -> Config:
    group: dict[str, Any] = {
        "name": "g1",
        "queries": [
            {
                "id": "metric_a",
                "promql": "up",
                "detectors": detectors,
            }
        ],
    }
    if ensemble is not None:
        group["ensemble"] = ensemble
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
            "warmup_policy": "emit_warming_up",
            "emit_baseline_stability": False,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [group],
    }
    return Config.model_validate(raw)


def _runner(cfg: Config, stub: StubSource) -> tuple[Runner, SnapshotStore, OperationalMetrics]:
    store = SnapshotStore()
    ops = OperationalMetrics()
    return Runner(cfg, stub, store, ops), store, ops


class _AlwaysHigh:
    """Test double: every score crosses the default threshold."""

    name: ClassVar[str] = "AlwaysHigh"
    description: ClassVar[str] = "fires every run"
    defaults: ClassVar[dict[str, Any]] = {}

    def fit_score(self, df: pd.DataFrame, freq: str, params: dict[str, Any]) -> pd.DataFrame:
        del freq, params
        return pd.DataFrame(
            [
                {
                    "timestamp": float(df["timestamp"].iloc[-1]),
                    "score": 9.9,
                    "baseline": 0.0,
                    "is_outside": True,
                }
            ]
        )


class _AlwaysLow:
    """Test double: never fires."""

    name: ClassVar[str] = "AlwaysLow"
    description: ClassVar[str] = "never fires"
    defaults: ClassVar[dict[str, Any]] = {}

    def fit_score(self, df: pd.DataFrame, freq: str, params: dict[str, Any]) -> pd.DataFrame:
        del freq, params
        return pd.DataFrame(
            [
                {
                    "timestamp": float(df["timestamp"].iloc[-1]),
                    "score": 0.1,
                    "baseline": 0.0,
                    "is_outside": False,
                }
            ]
        )


@pytest.fixture(autouse=True)
def _register_stubs() -> Any:
    registry.register(_AlwaysHigh)  # type: ignore[arg-type]
    registry.register(_AlwaysLow)  # type: ignore[arg-type]
    try:
        yield
    finally:
        registry.unregister("AlwaysHigh")
        registry.unregister("AlwaysLow")


@pytest.mark.asyncio
async def test_no_ensemble_block_emits_no_composite(stub_source: StubSource) -> None:
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _config(ensemble=None, detectors=[{"name": "MAD"}, {"name": "Hampel"}])
    runner, _store, _ops = _runner(cfg, stub_source)

    result = await runner.run_group("g1")
    metrics = {s.metric for s in result.samples}
    assert "anomaly_composite_score" not in metrics
    assert "anomaly_composite_outside_threshold" not in metrics


@pytest.mark.asyncio
async def test_ensemble_max_fires_when_any_detector_fires(stub_source: StubSource) -> None:
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "h"}, df)]))

    cfg = _config(
        ensemble={"method": "max"},
        detectors=[{"name": "AlwaysHigh"}, {"name": "AlwaysLow"}],
    )
    runner, _store, _ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")

    composite = [s for s in result.samples if s.metric == "anomaly_composite_outside_threshold"]
    assert len(composite) == 1
    assert composite[0].value == 1.0
    labels = dict(composite[0].labels)
    # Composite metrics carry only id/group/source labels — no detector
    # label, since the composite spans multiple detectors.
    assert labels["id"] == "metric_a"
    assert labels["group"] == "g1"
    assert "detector" not in labels
    assert labels["instance"] == "h"

    score = next(s for s in result.samples if s.metric == "anomaly_composite_score")
    # max-of-scores: AlwaysHigh dominates.
    assert score.value == pytest.approx(9.9)


@pytest.mark.asyncio
async def test_ensemble_max_clears_when_no_detector_fires(stub_source: StubSource) -> None:
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _config(
        ensemble={"method": "max"},
        detectors=[
            {"name": "AlwaysLow", "instance": "a"},
            {"name": "AlwaysLow", "instance": "b"},
        ],
    )
    runner, _store, _ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")

    composite = next(s for s in result.samples if s.metric == "anomaly_composite_outside_threshold")
    assert composite.value == 0.0


@pytest.mark.asyncio
async def test_ensemble_voting_requires_min_detectors(stub_source: StubSource) -> None:
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    # One firing detector + one quiet detector + min_detectors=2 -> does
    # not fire even though "max" would have fired.
    cfg = _config(
        ensemble={"method": "voting", "min_detectors": 2},
        detectors=[
            {"name": "AlwaysHigh"},
            {"name": "AlwaysLow"},
        ],
    )
    runner, _store, _ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")

    composite = next(s for s in result.samples if s.metric == "anomaly_composite_outside_threshold")
    assert composite.value == 0.0


@pytest.mark.asyncio
async def test_ensemble_voting_fires_when_threshold_met(stub_source: StubSource) -> None:
    df = point_spike(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    # Two firing detectors + min_detectors=2 -> fires.
    cfg = _config(
        ensemble={"method": "voting", "min_detectors": 2},
        detectors=[
            {"name": "AlwaysHigh", "instance": "a"},
            {"name": "AlwaysHigh", "instance": "b"},
        ],
    )
    runner, _store, _ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")

    composite = next(s for s in result.samples if s.metric == "anomaly_composite_outside_threshold")
    assert composite.value == 1.0


@pytest.mark.asyncio
async def test_ensemble_considers_suppressed_auto_select_losers(
    stub_source: StubSource,
) -> None:
    """auto_select hides losers from /metrics but the ensemble should
    still see every configured detector's verdict — otherwise voting
    silently degenerates to the single winner."""
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
            "warmup_policy": "emit_warming_up",
            "emit_baseline_stability": False,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g1",
                "auto_select": True,
                "ensemble": {"method": "voting", "min_detectors": 2},
                "queries": [
                    {
                        "id": "metric_a",
                        "promql": "up",
                        "detectors": [
                            {"name": "AlwaysHigh", "instance": "a"},
                            {"name": "AlwaysHigh", "instance": "b"},
                        ],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    runner, _store, _ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")

    score_samples = [s for s in result.samples if s.metric == "anomaly_score"]
    # auto_select keeps just one detector visible.
    assert len(score_samples) == 1
    # But the composite saw both run and fires.
    composite = next(s for s in result.samples if s.metric == "anomaly_composite_outside_threshold")
    assert composite.value == 1.0
