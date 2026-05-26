"""Synthetic-injection calibration cycle.

Used by two v0.2 features that share the same underlying primitive:

- **Auto-select** picks the best detector per series by F1 on injected
  anomalies.
- **Confidence score** + **baseline-stability gauge** surface per-detector
  trust signals derived from the same injection results.

A calibration cycle takes a rolling window of real samples for one
series, injects three canonical synthetic anomalies (point spike, step
change, level drift), and replays each configured detector against
those windows plus a clean control. The conservative default (one
sample per pattern, one clean control) yields four detector runs per
detector per series per cycle — enough signal to rank detectors without
flooding the TSDB or the worker pool.

Calibration is cached per ``(group, query, series_key, detector_name,
detector_instance)`` with a TTL (default 24h). Re-running on every
detector tick would be the wrong shape: the synthetic-injection
fixtures don't shift fast, and operators want predictability between
calibration windows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DetectorCalibration:
    """One detector's calibration result for one series.

    Stored as immutable so the snapshot store can hand it out without
    callers worrying about background updates clobbering local copies.
    """

    precision: float
    recall: float
    f1: float
    baseline_stability: float
    data_coverage: float
    confidence_score: float
    last_calibrated: float

    def as_dict(self) -> dict[str, float]:
        return {
            "precision": self.precision,
            "recall": self.recall,
            "f1": self.f1,
            "baseline_stability": self.baseline_stability,
            "data_coverage": self.data_coverage,
            "confidence_score": self.confidence_score,
            "last_calibrated": self.last_calibrated,
        }


def _f1(precision: float, recall: float) -> float:
    denom = precision + recall
    if denom <= 0.0:
        return 0.0
    return 2.0 * precision * recall / denom


def _inject_point_spike(values: np.ndarray) -> np.ndarray:
    out = values.copy()
    spread = float(np.std(values)) or 1.0
    out[-1] = float(np.median(values)) + 10.0 * spread
    return out


def _inject_step_change(values: np.ndarray) -> np.ndarray:
    out = values.copy()
    spread = float(np.std(values)) or 1.0
    # Step lands in the last 10% of the window so the latest sample is
    # squarely in the new regime — symmetric with the MAD test fixture
    # in tests/test_mad.py.
    change_at = max(1, int(0.9 * out.size))
    out[change_at:] = out[change_at:] + 5.0 * spread
    return out


def _inject_level_drift(values: np.ndarray) -> np.ndarray:
    out = values.copy()
    spread = float(np.std(values)) or 1.0
    drift = np.linspace(0.0, 4.0 * spread, num=out.size)
    return out + drift


INJECTION_PATTERNS: dict[str, Any] = {
    "point_spike": _inject_point_spike,
    "step_change": _inject_step_change,
    "level_drift": _inject_level_drift,
}


def _baseline_stability(samples: pd.DataFrame, detector: Any, params: dict[str, Any]) -> float:
    """How stable is the baseline within the rolling window?

    Splits the window in halves and runs the detector on each half;
    stability is 1 minus the normalised difference between the two
    baseline estimates, clipped to [0, 1]. Higher = more stable.

    This is a simple, robust proxy that works for any detector
    surfacing a ``baseline`` column, which the contract already
    guarantees.
    """
    n = len(samples)
    if n < 4:
        return 0.0
    mid = n // 2
    first = samples.iloc[:mid].reset_index(drop=True)
    second = samples.iloc[mid:].reset_index(drop=True)
    try:
        b1 = float(detector.fit_score(first, "", params).iloc[-1]["baseline"])
        b2 = float(detector.fit_score(second, "", params).iloc[-1]["baseline"])
    except Exception:
        return 0.0
    scale = max(abs(b1), abs(b2), 1e-9)
    diff = abs(b2 - b1) / scale
    return float(max(0.0, min(1.0, 1.0 - diff)))


def _detector_fires(
    detector: Any,
    samples: pd.DataFrame,
    params: dict[str, Any],
    threshold: float,
) -> bool:
    try:
        row = detector.fit_score(samples, "", params).iloc[-1]
    except Exception:
        return False
    return float(row.get("score", 0.0)) >= threshold


def calibrate_detector(
    *,
    samples: pd.DataFrame,
    detector_factory: Any,
    params: dict[str, Any],
    threshold: float,
    expected_samples: int,
    now: float,
) -> DetectorCalibration:
    """Run one synthetic-injection cycle for a single detector.

    Conservative cost: one injection per pattern (three patterns) plus
    one clean control. Four detector runs per detector per series per
    cycle. The runner caps invocation cadence via the calibration TTL
    so this stays manageable even on large groups.

    Args:
        samples: Clean rolling window (caller responsible for ensuring
            it represents normal behaviour — the injection assumes the
            window is otherwise anomaly-free).
        detector_factory: Callable returning a fresh detector instance
            per call (matches how the runner instantiates detectors).
        params: Merged detector params, same as the runner uses.
        threshold: Alert threshold (``alert_thresholds.score``) used to
            decide which scores count as "fires".
        expected_samples: Window-size in samples expected for
            full coverage. Coverage = ``min(1, len(samples) /
            expected)``.
        now: Calibration timestamp (wall-clock seconds since epoch).

    Returns:
        A :class:`DetectorCalibration` with the per-detector metrics
        used by both auto-select and the confidence gauge.
    """
    if samples.empty:
        return DetectorCalibration(
            precision=0.0,
            recall=0.0,
            f1=0.0,
            baseline_stability=0.0,
            data_coverage=0.0,
            confidence_score=0.0,
            last_calibrated=now,
        )
    values = samples["y"].to_numpy(dtype=float, copy=False)
    detector = detector_factory()
    # Clean control: detector should NOT fire on the unperturbed window.
    control_fires = _detector_fires(detector, samples, params, threshold)
    tp = 0
    fn = 0
    for pattern in INJECTION_PATTERNS.values():
        injected = pattern(values)
        injected_df = pd.DataFrame(
            {
                "timestamp": samples["timestamp"].to_numpy(copy=False),
                "y": injected,
            }
        )
        if _detector_fires(detector, injected_df, params, threshold):
            tp += 1
        else:
            fn += 1
    fp = 1 if control_fires else 0
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = _f1(precision, recall)
    stability = _baseline_stability(samples, detector, params)
    coverage = float(min(1.0, max(0.0, len(samples) / max(1, expected_samples))))
    confidence = 0.4 * precision + 0.3 * stability + 0.3 * coverage
    return DetectorCalibration(
        precision=precision,
        recall=recall,
        f1=f1,
        baseline_stability=stability,
        data_coverage=coverage,
        confidence_score=float(max(0.0, min(1.0, confidence))),
        last_calibrated=now,
    )


__all__ = [
    "INJECTION_PATTERNS",
    "DetectorCalibration",
    "calibrate_detector",
]
