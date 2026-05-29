"""Unit tests for the anomaly-typing classifier and frozen value-set."""

from __future__ import annotations

import numpy as np

from promanomaly.anomaly_type import (
    ANOMALY_TYPES,
    classify_window,
    coerce_declared_type,
)


def _classify(
    values: list[float],
    *,
    detector: str = "MAD",
    baseline: float = 0.0,
    change_point: bool = False,
    cohort: bool = False,
) -> str:
    arr = np.asarray(values, dtype=float)
    latest = float(arr[-1]) if arr.size else 0.0
    return classify_window(
        detector_name=detector,
        values=arr,
        baseline=baseline if baseline else float(np.median(arr)),
        latest_y=latest,
        change_point_fired=change_point,
        cohort_aware=cohort,
    )


def test_cohort_detector_is_cohort_divergence() -> None:
    assert _classify([1.0, 1.0, 5.0], detector="Cohort", cohort=True) == "cohort_divergence"


def test_change_point_fire_is_directional_step() -> None:
    rng = np.random.default_rng(0)
    base = list(rng.normal(10.0, 0.2, size=60))
    up = base + list(rng.normal(20.0, 0.2, size=20))
    assert (
        classify_window(
            detector_name="CUSUM",
            values=np.asarray(up, dtype=float),
            baseline=10.0,
            latest_y=20.0,
            change_point_fired=True,
            cohort_aware=False,
        )
        == "step_up"
    )
    assert (
        classify_window(
            detector_name="CUSUM",
            values=np.asarray(up, dtype=float),
            baseline=20.0,
            latest_y=5.0,
            change_point_fired=True,
            cohort_aware=False,
        )
        == "step_down"
    )


def test_point_spike_classification() -> None:
    rng = np.random.default_rng(1)
    vals = list(rng.normal(10.0, 0.1, size=80))
    vals[-1] = 30.0  # lone spike at the end
    assert _classify(vals) == "point_spike"


def test_step_change_classification() -> None:
    rng = np.random.default_rng(2)
    # First half ~10, abrupt jump to ~18 with low noise (not monotonic).
    head = list(rng.normal(10.0, 0.2, size=40))
    tail = list(rng.normal(18.0, 0.2, size=40))
    assert _classify(head + tail) == "step_up"


def test_level_drift_classification() -> None:
    # Strong monotonic ramp across the whole window.
    vals = list(np.linspace(10.0, 25.0, num=80))
    assert _classify(vals) == "level_drift"


def test_variance_shift_classification() -> None:
    rng = np.random.default_rng(3)
    head = list(rng.normal(10.0, 0.2, size=40))  # tight
    tail = list(rng.normal(10.0, 3.0, size=40))  # same mean, wide
    assert _classify(head + tail) == "variance_shift"


def test_short_window_defaults_to_point_spike() -> None:
    assert _classify([1.0, 2.0]) == "point_spike"


def test_all_produced_types_are_in_frozen_set() -> None:
    produced = {
        "point_spike",
        "step_up",
        "step_down",
        "level_drift",
        "variance_shift",
        "cohort_divergence",
    }
    assert produced <= ANOMALY_TYPES


def test_coerce_declared_type() -> None:
    assert coerce_declared_type("step_up") == "step_up"
    assert coerce_declared_type("not_a_type") is None
    assert coerce_declared_type(None) is None
    assert coerce_declared_type(42) is None
