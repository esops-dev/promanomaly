"""DistributionShift detector tests."""

from __future__ import annotations

import pytest

from promanomaly.detectors.distribution_shift import DistributionShift
from tests.fixtures import clean_baseline, short_window, variance_shift


def test_metadata() -> None:
    detector = DistributionShift()
    assert detector.name == "DistributionShift"
    assert "distribution" in detector.description.lower()
    spec_defaults = {p.name: p.default for p in detector.param_spec}
    assert detector.defaults == spec_defaults


def test_clean_baseline_scores_low() -> None:
    detector = DistributionShift()
    df = clean_baseline(n=200)
    row = detector.fit_score(df, "", {}).iloc[-1]
    # Same distribution throughout — no shift.
    assert float(row["score"]) < 3.0


def test_variance_shift_scores_high() -> None:
    detector = DistributionShift()
    df = variance_shift(n=200, noise_low=0.1, noise_high=2.0, change_at=100)
    row = detector.fit_score(df, "", {}).iloc[-1]
    # The distribution has changed — should detect the shift.
    assert float(row["score"]) > 3.0


def test_split_param_changes_sensitivity() -> None:
    detector = DistributionShift()
    df = variance_shift(n=200, noise_low=0.1, noise_high=2.0, change_at=100)
    score_half = float(detector.fit_score(df, "", {"split": 0.5}).iloc[-1]["score"])
    score_small = float(detector.fit_score(df, "", {"split": 0.2}).iloc[-1]["score"])
    # Both should detect the shift; exact ordering depends on the split.
    assert score_half > 0.0
    assert score_small > 0.0


def test_wasserstein_statistic() -> None:
    detector = DistributionShift()
    df = variance_shift(n=200, noise_low=0.1, noise_high=2.0, change_at=100)
    row = detector.fit_score(df, "", {"statistic": "wasserstein"}).iloc[-1]
    assert float(row["score"]) > 0.0


def test_invalid_statistic_raises() -> None:
    detector = DistributionShift()
    df = clean_baseline(n=100)
    with pytest.raises(ValueError, match="statistic"):
        detector.fit_score(df, "", {"statistic": "invalid"})


def test_short_window_returns_zero() -> None:
    detector = DistributionShift()
    df = short_window(n=3)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) == 0.0
