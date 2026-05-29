"""Unit tests for the operator-facing severity signal."""

from __future__ import annotations

import pytest

from promanomaly.severity import (
    compute_severity,
    duration_ramp,
    squash_strength,
)


def test_squash_strength_sigma_detector_saturates_at_twice_threshold() -> None:
    # At the threshold, strength is half; at 2x threshold, fully saturated.
    assert squash_strength(3.0, 3.0, "MAD") == pytest.approx(0.5)
    assert squash_strength(6.0, 3.0, "MAD") == pytest.approx(1.0)
    assert squash_strength(12.0, 3.0, "MAD") == pytest.approx(1.0)  # clamped
    assert squash_strength(0.0, 3.0, "MAD") == pytest.approx(0.0)


def test_squash_strength_probability_detector_passes_through() -> None:
    assert squash_strength(0.6, 0.5, "BOCPD") == pytest.approx(0.6)
    assert squash_strength(1.5, 0.5, "BOCPD") == pytest.approx(1.0)  # clamped


def test_squash_strength_degenerate_threshold() -> None:
    assert squash_strength(0.1, 0.0, "MAD") == 1.0
    assert squash_strength(0.0, 0.0, "MAD") == 0.0


def test_duration_ramp_saturates_at_30_minutes() -> None:
    assert duration_ramp(0.0) == 0.0
    assert duration_ramp(900.0) == pytest.approx(0.5)
    assert duration_ramp(1800.0) == pytest.approx(1.0)
    assert duration_ramp(3600.0) == pytest.approx(1.0)


def test_severity_zero_when_not_firing() -> None:
    # A high raw score that was gated out (sub-threshold / practical floor)
    # must not rank above a real firing.
    assert (
        compute_severity(
            score=10.0,
            threshold=3.0,
            detector_name="MAD",
            outside=False,
            confidence=1.0,
            baseline_stability=1.0,
            duration_seconds=10_000.0,
        )
        == 0.0
    )


def test_severity_worked_example() -> None:
    # Matches docs/severity.md: MAD score 4.5, threshold 3.0,
    # confidence 0.8, stability 0.6, 15 minutes sustained.
    sev = compute_severity(
        score=4.5,
        threshold=3.0,
        detector_name="MAD",
        outside=True,
        confidence=0.8,
        baseline_stability=0.6,
        duration_seconds=900.0,
    )
    assert sev == pytest.approx(0.75 * 0.805, rel=1e-6)


def test_severity_uncalibrated_defaults_to_neutral() -> None:
    # No calibration yet (confidence/stability None) -> multiplier uses
    # neutral 1.0 so a strong firing still ranks high (not suppressed).
    sev = compute_severity(
        score=6.0,
        threshold=3.0,
        detector_name="MAD",
        outside=True,
        confidence=None,
        baseline_stability=None,
        duration_seconds=0.0,
    )
    # strength 1.0, multiplier 0.4 + 0.3 + 0.15 + 0 = 0.85
    assert sev == pytest.approx(0.85)


def test_severity_never_exceeds_strength() -> None:
    sev = compute_severity(
        score=6.0,
        threshold=3.0,
        detector_name="MAD",
        outside=True,
        confidence=1.0,
        baseline_stability=1.0,
        duration_seconds=10_000.0,
    )
    assert sev == pytest.approx(1.0)
    assert 0.0 <= sev <= 1.0


def test_severity_is_bounded() -> None:
    for score in (0.1, 3.0, 100.0):
        sev = compute_severity(
            score=score,
            threshold=3.0,
            detector_name="MAD",
            outside=True,
            confidence=0.5,
            baseline_stability=0.5,
            duration_seconds=300.0,
        )
        assert 0.0 <= sev <= 1.0
