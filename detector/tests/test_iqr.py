"""IQR detector tests."""

from __future__ import annotations

import pytest

from promanomaly.detectors.iqr import IQR
from tests.fixtures import clean_baseline, point_spike


def test_iqr_metadata() -> None:
    detector = IQR()
    assert detector.name == "IQR"
    assert detector.defaults == {"k": 1.5}


def test_clean_baseline_scores_zero() -> None:
    detector = IQR()
    df = clean_baseline()
    row = detector.fit_score(df, "", {}).iloc[-1]
    # The latest in-distribution sample is inside the Tukey band, so
    # the score is exactly 0 by construction.
    assert float(row["score"]) == 0.0


def test_point_spike_scores_high() -> None:
    detector = IQR()
    df = point_spike()
    row = detector.fit_score(df, "", {}).iloc[-1]
    # 10-sigma spike on top of ~0.1 noise sits far outside the band.
    assert float(row["score"]) > 3.0


def test_baseline_returns_median() -> None:
    detector = IQR()
    df = clean_baseline(mean=42.0, noise=0.01)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert abs(float(row["baseline"]) - 42.0) < 0.05


def test_k_param_changes_sensitivity() -> None:
    # A wider band (k=3) should yield a lower (or equal) score for the
    # same spike — the spike has to travel further to escape the band.
    detector = IQR()
    df = point_spike()
    narrow = detector.fit_score(df, "", {"k": 1.5}).iloc[-1]
    wide = detector.fit_score(df, "", {"k": 3.0}).iloc[-1]
    assert float(wide["score"]) <= float(narrow["score"])


def test_invalid_k_raises() -> None:
    detector = IQR()
    df = clean_baseline()
    with pytest.raises(ValueError):
        detector.fit_score(df, "", {"k": -1.0})
    with pytest.raises(ValueError):
        detector.fit_score(df, "", {"k": "huh"})
