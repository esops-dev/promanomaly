"""Calibration cycle tests (auto-select + confidence-score primitives)."""

from __future__ import annotations

import time

from promanomaly.calibration import (
    INJECTION_PATTERNS,
    DetectorCalibration,
    calibrate_detector,
)
from promanomaly.detectors.iqr import IQR
from promanomaly.detectors.mad import MAD
from tests.fixtures import clean_baseline


def test_injection_patterns_registered() -> None:
    assert set(INJECTION_PATTERNS) == {"point_spike", "step_change", "level_drift"}


def test_calibration_returns_metrics_in_unit_range() -> None:
    df = clean_baseline(noise=0.05)
    cal = calibrate_detector(
        samples=df,
        detector_factory=MAD,
        params={},
        threshold=3.0,
        expected_samples=120,
        now=time.time(),
    )
    assert isinstance(cal, DetectorCalibration)
    for name in ("precision", "recall", "f1", "baseline_stability", "confidence_score"):
        value = getattr(cal, name)
        assert 0.0 <= value <= 1.0, f"{name} out of range: {value}"
    assert cal.data_coverage == 1.0


def test_calibration_recall_high_on_well_tuned_detector() -> None:
    # MAD has strong recall on every synthetic pattern at the default
    # threshold; recall must be > 0 or the calibration is broken.
    df = clean_baseline(noise=0.05)
    cal = calibrate_detector(
        samples=df,
        detector_factory=MAD,
        params={},
        threshold=3.0,
        expected_samples=120,
        now=time.time(),
    )
    assert cal.recall > 0.0
    assert cal.f1 > 0.0


def test_calibration_empty_input_returns_zeroed_result() -> None:
    import pandas as pd

    empty = pd.DataFrame({"timestamp": [], "y": []})
    cal = calibrate_detector(
        samples=empty,
        detector_factory=MAD,
        params={},
        threshold=3.0,
        expected_samples=120,
        now=12345.0,
    )
    assert cal.precision == cal.recall == cal.f1 == 0.0
    assert cal.confidence_score == 0.0
    assert cal.last_calibrated == 12345.0


def test_iqr_calibration_runs() -> None:
    # Smoke: a second detector class should also round-trip through
    # calibration without error.
    df = clean_baseline(noise=0.05)
    cal = calibrate_detector(
        samples=df,
        detector_factory=IQR,
        params={"k": 1.5},
        threshold=3.0,
        expected_samples=120,
        now=time.time(),
    )
    assert isinstance(cal, DetectorCalibration)
    assert cal.data_coverage == 1.0


def test_calibration_respects_detector_is_outside() -> None:
    """A detector that flags ``is_outside=True`` in its score row (e.g.
    BOCPD, CUSUM with a fresh crossing) must count as a fire in
    calibration even when its raw score is in different units from the
    threshold. Without this, BOCPD's probability score (max 1.0) would
    never reach the default 3.0 sigma-multiplier threshold and its F1
    would always be zero — making auto-select silently rank it last."""
    from promanomaly.detectors.bocpd import BOCPD

    df = clean_baseline(n=120, mean=10.0, noise=0.1)
    # Use BOCPD's own threshold (0.5) so the calibration math matches
    # the runner's effective-threshold resolution.
    cal = calibrate_detector(
        samples=df,
        detector_factory=BOCPD,
        params={"threshold": 0.5, "hazard": 250, "lag": 15},
        threshold=0.5,
        expected_samples=120,
        now=time.time(),
    )
    # BOCPD detects the synthetic step change / point spike injections,
    # so recall must be > 0.
    assert cal.recall > 0.0, (
        f"BOCPD calibration recall is 0 — calibration probably isn't honouring is_outside: {cal!r}"
    )
