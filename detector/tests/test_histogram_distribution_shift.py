"""HistogramDistributionShift detector tests."""

from __future__ import annotations

import pandas as pd
import pytest

from promanomaly.detectors.histogram_distribution_shift import HistogramDistributionShift
from tests.fixtures import clean_baseline, variance_shift


def test_metadata() -> None:
    detector = HistogramDistributionShift()
    assert detector.name == "HistogramDistributionShift"
    assert "histogram" in detector.description.lower()
    spec_defaults = {p.name: p.default for p in detector.param_spec}
    assert detector.defaults == spec_defaults


def test_fallback_clean_baseline_scores_low() -> None:
    """Without ``le`` column, falls back to raw-value distribution shift."""
    detector = HistogramDistributionShift()
    df = clean_baseline(n=200)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) < 3.0


def test_fallback_variance_shift_scores_high() -> None:
    """Without ``le`` column, falls back to raw-value distribution shift."""
    detector = HistogramDistributionShift()
    df = variance_shift(n=200, noise_low=0.1, noise_high=2.0, change_at=100)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) > 3.0


def _histogram_frame(
    n_timestamps: int = 20,
    buckets: list[float] | None = None,
    shift_at: int | None = None,
) -> pd.DataFrame:
    """Build a histogram-shaped DataFrame with ``le`` column.

    Without shift: all weight in the 0.1 bucket.
    With shift: weight moves to the 1.0 bucket after ``shift_at``.
    """
    if buckets is None:
        buckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    rows: list[dict[str, float]] = []
    for t in range(n_timestamps):
        ts = float(t * 15)
        shifted = shift_at is not None and t >= shift_at
        for i, le in enumerate(buckets):
            if shifted:
                # CDF concentrated at the high end.
                cdf = 0.0 if le < 0.5 else float(i + 1) / len(buckets)
            else:
                # CDF concentrated at the low end.
                cdf = float(i + 1) / len(buckets)
            rows.append({"timestamp": ts, "y": cdf, "le": le})
    return pd.DataFrame(rows)


def test_histogram_no_shift_scores_low() -> None:
    detector = HistogramDistributionShift()
    df = _histogram_frame(n_timestamps=20)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) < 3.0


def test_histogram_with_shift_scores_high() -> None:
    detector = HistogramDistributionShift()
    df = _histogram_frame(n_timestamps=20, shift_at=10)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) > 0.0


def test_invalid_statistic_raises() -> None:
    detector = HistogramDistributionShift()
    df = clean_baseline(n=100)
    with pytest.raises(ValueError, match="statistic"):
        detector.fit_score(df, "", {"statistic": "invalid"})
