"""Calibration interaction tests for stratified detectors.

The synthetic-injection calibration cycle was designed for rolling-
window detectors and uses three patterns — point spike, step change,
level drift. Two of those (step change, level drift) mutate the
trailing slice of the window; on a bucket-based detector that slice
overlaps the very buckets the detector medians against, so the
injected anomaly partially contaminates its own baseline.

These tests pin what *does* work (the cycle completes cleanly, produces
in-range numbers, and the point-spike pattern is reliably detected
against a same-bucket baseline) and document the known limitation:
``f1`` for a stratified detector should be read as "imperfect lower
bound" rather than "ground truth".
"""

from __future__ import annotations

import time
from typing import Any

import numpy as np
import pandas as pd
import pytest

from promanomaly.calibration import calibrate_detector
from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.detectors.day_of_week_mad import DayOfWeekMAD
from promanomaly.detectors.hour_of_day_mad import HourOfDayMAD
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series


def _diurnal_history(*, days: int, step_seconds: float = 300.0) -> pd.DataFrame:
    n = int(days * 86400 / step_seconds)
    base_ts = 1_700_000_000.0 - (1_700_000_000.0 % 86400.0)
    timestamps = base_ts + np.arange(n) * step_seconds
    seconds_in_day = timestamps % 86400.0
    values = 5.0 * np.sin(2.0 * np.pi * seconds_in_day / 86400.0)
    rng = np.random.default_rng(0)
    values = values + rng.normal(0.0, 0.05, size=n)
    return pd.DataFrame({"timestamp": timestamps, "y": values})


def test_calibration_completes_for_day_of_week_mad() -> None:
    # The fundamental contract: the cycle returns a DetectorCalibration
    # with in-range numbers and never raises, even on the heavy
    # multi-week window a stratified detector wants to see.
    samples = _diurnal_history(days=21, step_seconds=300.0)
    cal = calibrate_detector(
        samples=samples,
        detector_factory=DayOfWeekMAD,
        params=dict(DayOfWeekMAD.defaults),
        threshold=3.0,
        expected_samples=21 * 24 * 12,
        now=time.time(),
    )
    assert 0.0 <= cal.precision <= 1.0
    assert 0.0 <= cal.recall <= 1.0
    assert 0.0 <= cal.f1 <= 1.0
    assert 0.0 <= cal.baseline_stability <= 1.0
    assert 0.0 <= cal.data_coverage <= 1.0
    assert 0.0 <= cal.confidence_score <= 1.0


def test_calibration_completes_for_hour_of_day_mad() -> None:
    samples = _diurnal_history(days=7, step_seconds=300.0)
    cal = calibrate_detector(
        samples=samples,
        detector_factory=HourOfDayMAD,
        params=dict(HourOfDayMAD.defaults),
        threshold=3.0,
        expected_samples=7 * 24 * 12,
        now=time.time(),
    )
    assert 0.0 <= cal.f1 <= 1.0
    # ``baseline_stability`` for stratified detectors is a known
    # limitation: the proxy splits the window in halves and compares
    # the detector's ``baseline`` for the last sample of each half. For
    # a stratified detector those two latest samples land in different
    # hour-of-day buckets with naturally different bucket medians, so
    # the difference is large even on a perfectly clean signal and
    # stability collapses toward 0. Documented in
    # ``docs/scaling-stratified.md``; the test pins the in-range
    # behaviour rather than claiming meaningfulness.
    assert 0.0 <= cal.baseline_stability <= 1.0


def test_stratified_point_spike_fires_in_calibration() -> None:
    # The point-spike pattern (latest sample only) is the canonical case
    # for a stratified detector — the injected outlier lives in a
    # single bucket and the historical samples in that bucket remain
    # clean. We pin this end-to-end via ``calibrate_detector`` so a
    # regression in either the detector or the injection helpers is
    # caught here, not silently absorbed into a lower F1.
    from promanomaly.calibration import _detector_fires, _inject_point_spike

    samples = _diurnal_history(days=21, step_seconds=300.0)
    values = samples["y"].to_numpy(dtype=float, copy=False)
    injected = _inject_point_spike(values)
    injected_df = pd.DataFrame(
        {
            "timestamp": samples["timestamp"].to_numpy(copy=False),
            "y": injected,
        }
    )
    assert _detector_fires(
        DayOfWeekMAD(),
        injected_df,
        dict(DayOfWeekMAD.defaults),
        threshold=3.0,
    )


@pytest.mark.asyncio
async def test_calibration_runs_through_runner_for_stratified_detector(
    stub_source: StubSource,
) -> None:
    # End-to-end: spin up the runner with a stratified detector,
    # ``emit_baseline_stability: true`` (the default that activates
    # calibration), and verify the run produces calibration-derived
    # metrics — confidence_score + baseline_stability — without
    # crashing on the multi-week window.
    history = _diurnal_history(days=21, step_seconds=300.0)
    series = make_series({"instance": "h"}, history)
    stub_source.respond(lambda _: StubResponse(series=[series]))

    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "5m",
            "step": "5m",
            "min_points": 5,
            "emit_baseline_stability": True,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g_strat",
                "queries": [
                    {
                        "id": "weekly",
                        "promql": "rate(metric[5m])",
                        "detectors": [
                            {
                                "name": "DayOfWeekMAD",
                                "params": {"baseline_refresh_interval": "1h"},
                            }
                        ],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)

    result = await runner.run_group("g_strat")
    assert result.succeeded

    metrics_emitted = {s.metric for s in result.samples}
    # The score-family is always present; calibration adds the two
    # confidence gauges only when calibration actually ran for this
    # series (which the default ``emit_baseline_stability=true``
    # triggers on the first pass).
    assert {"anomaly_score", "anomaly_baseline"}.issubset(metrics_emitted)
    assert {"anomaly_confidence_score", "anomaly_baseline_stability"}.issubset(metrics_emitted), (
        "calibration-derived gauges should be emitted on the first stratified pass"
    )

    confidence = next(s for s in result.samples if s.metric == "anomaly_confidence_score")
    stability = next(s for s in result.samples if s.metric == "anomaly_baseline_stability")
    assert 0.0 <= confidence.value <= 1.0
    assert 0.0 <= stability.value <= 1.0
