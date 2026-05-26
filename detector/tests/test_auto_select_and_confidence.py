"""Runner tests for auto-select, confidence/stability emission, and the
per-detector query-window override used by stratified detectors."""

from __future__ import annotations

import time
from typing import Any, ClassVar

import pandas as pd
import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline


def _make_config(
    *,
    detectors: list[dict[str, Any]],
    auto_select: Any = False,
    auto_select_interval: str = "24h",
    emit_baseline_stability: bool = True,
    min_points: int = 30,
) -> Config:
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": min_points,
            "warmup_policy": "emit_warming_up",
            "emit_baseline_stability": emit_baseline_stability,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g1",
                "auto_select": auto_select,
                "auto_select_interval": auto_select_interval,
                "queries": [
                    {
                        "id": "metric_a",
                        "promql": "up",
                        "detectors": detectors,
                    }
                ],
            }
        ],
    }
    return Config.model_validate(raw)


def _runner(cfg: Config, stub: StubSource) -> tuple[Runner, SnapshotStore, OperationalMetrics]:
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub, store, ops)
    return runner, store, ops


@pytest.mark.asyncio
async def test_confidence_and_stability_emitted_when_enabled(stub_source: StubSource) -> None:
    df = clean_baseline(n=120, noise=0.1)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "h1"}, df)]))

    cfg = _make_config(detectors=[{"name": "MAD"}])
    runner, _store, _ops = _runner(cfg, stub_source)

    result = await runner.run_group("g1")
    metrics = {s.metric for s in result.samples}
    assert "anomaly_confidence_score" in metrics
    assert "anomaly_baseline_stability" in metrics
    confidence = next(s for s in result.samples if s.metric == "anomaly_confidence_score")
    stability = next(s for s in result.samples if s.metric == "anomaly_baseline_stability")
    assert 0.0 <= confidence.value <= 1.0
    assert 0.0 <= stability.value <= 1.0


@pytest.mark.asyncio
async def test_baseline_stability_suppressible(stub_source: StubSource) -> None:
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _make_config(detectors=[{"name": "MAD"}], emit_baseline_stability=False)
    runner, _store, _ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    metrics = {s.metric for s in result.samples}
    # With emit_baseline_stability=False and auto_select=False, no
    # calibration runs at all — confidence shouldn't appear either.
    assert "anomaly_baseline_stability" not in metrics
    assert "anomaly_confidence_score" not in metrics


@pytest.mark.asyncio
async def test_auto_select_emits_only_winner_by_default(stub_source: StubSource) -> None:
    df = clean_baseline(n=120, noise=0.1)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _make_config(
        detectors=[{"name": "MAD"}, {"name": "Hampel", "params": {"t0": 3}}],
        auto_select=True,
    )
    runner, _store, _ops = _runner(cfg, stub_source)

    result = await runner.run_group("g1")
    score_samples = [s for s in result.samples if s.metric == "anomaly_score"]
    # Two detectors were configured but auto_select=True should keep
    # only one winner per series.
    assert len(score_samples) == 1
    labels = dict(score_samples[0].labels)
    assert labels["best_detector"] == labels["detector"]
    assert labels["detector"] in {"MAD", "Hampel"}


@pytest.mark.asyncio
async def test_auto_select_explicit_plus_best_keeps_all(stub_source: StubSource) -> None:
    df = clean_baseline(n=120, noise=0.1)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _make_config(
        detectors=[{"name": "MAD"}, {"name": "Hampel"}],
        auto_select="explicit_plus_best",
    )
    runner, _store, _ops = _runner(cfg, stub_source)

    result = await runner.run_group("g1")
    score_samples = [s for s in result.samples if s.metric == "anomaly_score"]
    # explicit_plus_best emits both detectors, tags the winner with
    # best_detector= but does not tag the loser.
    assert len(score_samples) == 2
    winners = [s for s in score_samples if "best_detector" in dict(s.labels)]
    losers = [s for s in score_samples if "best_detector" not in dict(s.labels)]
    assert len(winners) == 1
    assert len(losers) == 1


@pytest.mark.asyncio
async def test_calibration_cached_across_runs(stub_source: StubSource) -> None:
    df = clean_baseline(n=120, noise=0.1)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _make_config(detectors=[{"name": "MAD"}], auto_select_interval="24h")
    runner, store, _ops = _runner(cfg, stub_source)

    await runner.run_group("g1")
    cal1 = store.get_calibration("g1", "metric_a", (), "MAD", None)
    assert cal1 is not None
    first_ts = cal1.last_calibrated

    # Second run within the TTL should not re-calibrate (timestamp
    # unchanged). Wait a non-trivial gap so any accidental refresh is
    # visible.
    time.sleep(0.05)
    await runner.run_group("g1")
    cal2 = store.get_calibration("g1", "metric_a", (), "MAD", None)
    assert cal2 is not None
    assert cal2.last_calibrated == first_ts


@pytest.mark.asyncio
async def test_calibration_refreshes_after_ttl(stub_source: StubSource) -> None:
    df = clean_baseline(n=120, noise=0.1)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    # 1ms TTL — every run should re-calibrate.
    cfg = _make_config(detectors=[{"name": "MAD"}], auto_select_interval="1ms")
    runner, store, _ops = _runner(cfg, stub_source)

    await runner.run_group("g1")
    cal1 = store.get_calibration("g1", "metric_a", (), "MAD", None)
    assert cal1 is not None
    first_ts = cal1.last_calibrated

    time.sleep(0.05)
    await runner.run_group("g1")
    cal2 = store.get_calibration("g1", "metric_a", (), "MAD", None)
    assert cal2 is not None
    assert cal2.last_calibrated > first_ts


@pytest.mark.asyncio
async def test_per_detector_query_window_widens_fetch(stub_source: StubSource) -> None:
    # HourOfDayMAD should widen the fetched window to its lookback even
    # when the group default is much shorter. Listed alongside MAD so
    # the runner picks the larger of the two.
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _make_config(
        detectors=[
            {"name": "MAD"},
            {"name": "HourOfDayMAD", "params": {"lookback": "7d"}},
        ],
    )
    runner, _store, _ops = _runner(cfg, stub_source)

    await runner.run_group("g1")
    # The stub records every query made; the fetched window should be
    # at least the HourOfDayMAD lookback in seconds.
    assert stub_source.queries, "expected the runner to issue a query"
    fetched_window = stub_source.queries[-1]["window"]
    assert fetched_window >= 7 * 86400.0


@pytest.mark.asyncio
async def test_short_window_detector_only_sees_its_own_slice(
    stub_source: StubSource,
) -> None:
    # When MAD shares a query with HourOfDayMAD, the runner widens the
    # fetch to 7d — but MAD's rolling median must still be computed
    # over the last ``defaults.window`` seconds only. Otherwise MAD's
    # semantics silently change when a stratified detector is added
    # to the same query.
    import numpy as np
    import pandas as pd

    # Build a 7d series at 1m step where the FIRST half is far above
    # zero and the LAST 15m is essentially constant at zero. If MAD
    # sees the full 7d its median is non-zero; if it sees only the
    # last 15m its median is ~0.
    n_total = 7 * 24 * 60
    step = 60.0
    base_ts = 1_700_000_000.0
    timestamps = base_ts + np.arange(n_total) * step
    values = np.full(n_total, 100.0)
    last_15m_samples = int(15 * 60 / step)
    values[-last_15m_samples:] = 0.0
    df = pd.DataFrame({"timestamp": timestamps, "y": values})
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _make_config(
        detectors=[
            {"name": "MAD"},
            {"name": "HourOfDayMAD", "params": {"lookback": "7d"}},
        ],
    )
    runner, _store, _ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")

    mad_baseline = next(
        s
        for s in result.samples
        if s.metric == "anomaly_baseline" and dict(s.labels)["detector"] == "MAD"
    )
    # MAD's slice is the last 15m of zeros → median should be 0,
    # not 100 (which is what the full 7d median would be).
    assert mad_baseline.value == 0.0


class _CountingDetector:
    """Test double that counts fit_score invocations on a class-level
    dict so tests can compare runtime cost across configurations."""

    name: ClassVar[str] = ""  # set by subclass
    description: ClassVar[str] = "counts invocations"
    defaults: ClassVar[dict[str, Any]] = {}
    counters: ClassVar[dict[str, int]] = {}

    @classmethod
    def reset(cls) -> None:
        cls.counters.clear()

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        del freq, params
        self.__class__.counters[self.name] = self.__class__.counters.get(self.name, 0) + 1
        return pd.DataFrame(
            [
                {
                    "timestamp": float(df["timestamp"].iloc[-1]),
                    "score": 0.0,
                    "baseline": 0.0,
                    "is_outside": False,
                }
            ]
        )


class _Counter1(_CountingDetector):
    name: ClassVar[str] = "Counter1"


class _Counter2(_CountingDetector):
    name: ClassVar[str] = "Counter2"


@pytest.mark.asyncio
async def test_auto_select_skips_losers_when_no_ensemble(stub_source: StubSource) -> None:
    """auto_select: true without ensemble must not pay the compute for
    losers — the historical optimization. Both detectors share the same
    calibration cost; the winner additionally gets one runtime
    invocation, the loser zero. So winner_count == loser_count + 1."""
    from promanomaly.detectors import registry

    _CountingDetector.reset()
    registry.register(_Counter1)  # type: ignore[arg-type]
    registry.register(_Counter2)  # type: ignore[arg-type]
    try:
        df = clean_baseline(n=120)
        stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

        cfg = _make_config(
            detectors=[{"name": "Counter1"}, {"name": "Counter2"}],
            auto_select=True,
        )
        runner, _store, _ops = _runner(cfg, stub_source)
        await runner.run_group("g1")

        c1 = _CountingDetector.counters.get("Counter1", 0)
        c2 = _CountingDetector.counters.get("Counter2", 0)
        # Exactly one detector ran the runtime path; the other was
        # skipped. abs difference is 1.
        assert abs(c1 - c2) == 1, f"counters: {_CountingDetector.counters}"
    finally:
        registry.unregister("Counter1")
        registry.unregister("Counter2")
        _CountingDetector.reset()


@pytest.mark.asyncio
async def test_auto_select_runs_losers_when_ensemble_configured(
    stub_source: StubSource,
) -> None:
    """With an ensemble block, every detector must run so the fuser
    sees every vote — even if auto_select suppresses some from
    /metrics. Both detectors get the same calibration + runtime cost
    so their counters are equal."""
    from promanomaly.config import CURRENT_API_VERSION, Config
    from promanomaly.detectors import registry

    _CountingDetector.reset()
    registry.register(_Counter1)  # type: ignore[arg-type]
    registry.register(_Counter2)  # type: ignore[arg-type]
    try:
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
                    "ensemble": {"method": "max"},
                    "queries": [
                        {
                            "id": "metric_a",
                            "promql": "up",
                            "detectors": [{"name": "Counter1"}, {"name": "Counter2"}],
                        }
                    ],
                }
            ],
        }
        cfg = Config.model_validate(raw)
        runner, _store, _ops = _runner(cfg, stub_source)
        await runner.run_group("g1")

        c1 = _CountingDetector.counters.get("Counter1", 0)
        c2 = _CountingDetector.counters.get("Counter2", 0)
        assert c1 == c2, f"counters: {_CountingDetector.counters}"
        assert c1 > 0
    finally:
        registry.unregister("Counter1")
        registry.unregister("Counter2")
        _CountingDetector.reset()


@pytest.mark.asyncio
async def test_reload_clears_calibration_cache(stub_source: StubSource) -> None:
    # A reload that changes detector params must invalidate cached
    # calibration; the cache key does not include params so reuse
    # would otherwise apply stale F1/confidence numbers for up to
    # one auto_select_interval.
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _make_config(detectors=[{"name": "MAD"}])
    runner, store, _ops = _runner(cfg, stub_source)
    await runner.run_group("g1")
    assert store.get_calibration("g1", "metric_a", (), "MAD", None) is not None

    # Same group, "reloaded" config — calibration cache should be wiped.
    new_cfg = _make_config(detectors=[{"name": "MAD"}])
    runner.replace_config(new_cfg)
    assert store.get_calibration("g1", "metric_a", (), "MAD", None) is None
