"""ZScoreEWMA detector tests."""

from __future__ import annotations

import pytest

from promanomaly.detectors.zscore_ewma import ZScoreEWMA
from tests.fixtures import clean_baseline, gradual_drift, point_spike


def test_zscore_ewma_metadata() -> None:
    detector = ZScoreEWMA()
    assert detector.name == "ZScoreEWMA"
    assert detector.defaults == {"alpha": 0.05}
    assert "EWMA" in detector.description or "exponentially" in detector.description.lower()


def test_clean_baseline_scores_low() -> None:
    detector = ZScoreEWMA()
    df = clean_baseline()
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) < 3.0


def test_point_spike_scores_high() -> None:
    detector = ZScoreEWMA()
    df = point_spike()
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) > 3.0


def test_baseline_tracks_drift() -> None:
    # EWMA explicitly handles drift. The baseline at the end of a drift
    # window should be closer to the latest value than the very first
    # sample is.
    detector = ZScoreEWMA()
    df = gradual_drift(slope=0.1, noise=0.01)
    row = detector.fit_score(df, "", {"alpha": 0.2}).iloc[-1]
    baseline = float(row["baseline"])
    latest = float(df["y"].iloc[-1])
    first = float(df["y"].iloc[0])
    assert abs(baseline - latest) < abs(baseline - first)


def test_alpha_out_of_range_raises() -> None:
    detector = ZScoreEWMA()
    df = clean_baseline()
    with pytest.raises(ValueError):
        detector.fit_score(df, "", {"alpha": 0.0})
    with pytest.raises(ValueError):
        detector.fit_score(df, "", {"alpha": 1.5})
    with pytest.raises(ValueError):
        detector.fit_score(df, "", {"alpha": "not-a-number"})
