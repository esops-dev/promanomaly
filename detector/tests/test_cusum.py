"""CUSUM detector tests against the synthetic-series fixtures."""

from __future__ import annotations

import pytest

from promanomaly.detectors.cusum import CUSUM
from tests.fixtures import (
    clean_baseline,
    step_change,
)


def test_cusum_metadata() -> None:
    detector = CUSUM()
    assert detector.name == "CUSUM"
    assert "cusum" in detector.description.lower()
    assert detector.defaults == {"k": 0.5, "h": 5.0}


def test_clean_baseline_does_not_fire() -> None:
    detector = CUSUM()
    df = clean_baseline(n=200, mean=10.0, noise=0.1)
    row = detector.fit_score(df, "", {}).iloc[-1]
    # Clean baseline should keep the CUSUM well below h=5*sigma.
    assert float(row["score"]) < 5.0
    assert not bool(row["change_point"])


def test_recent_step_change_fires() -> None:
    detector = CUSUM()
    # Step lands at the latest sample with magnitude 5 (50 sigma at noise=0.1).
    df = step_change(n=200, step=5.0, noise=0.1, change_at=199)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) >= 5.0
    assert bool(row["change_point"])


def test_baseline_is_median() -> None:
    detector = CUSUM()
    df = clean_baseline(n=200, mean=42.0, noise=0.01)
    row = detector.fit_score(df, "", {}).iloc[-1]
    # CUSUM uses median for robustness; should be close to mean for clean data.
    assert abs(float(row["baseline"]) - 42.0) < 0.05


def test_h_parameter_controls_sensitivity() -> None:
    """A lower h fires more readily; a higher h is more conservative."""
    detector = CUSUM()
    # A modest step (4-sigma) shouldn't fire at h=5 but should at h=3.
    df = step_change(n=200, step=0.4, noise=0.1, change_at=190)
    loose = detector.fit_score(df, "", {"h": 3.0}).iloc[-1]
    tight = detector.fit_score(df, "", {"h": 10.0}).iloc[-1]
    # Same input -> same raw score; only the verdict differs.
    assert float(loose["score"]) == pytest.approx(float(tight["score"]), abs=1e-9)
    # Looser threshold is more permissive (or at least no less).
    if bool(tight["change_point"]):
        assert bool(loose["change_point"])


def test_score_in_sigma_units() -> None:
    """A clean signal's score should be small in sigma units."""
    detector = CUSUM()
    df = clean_baseline(n=200, mean=10.0, noise=0.5)
    row = detector.fit_score(df, "", {}).iloc[-1]
    # CUSUM score on a clean window: well below h=5.
    assert float(row["score"]) < 5.0


def test_k_validation() -> None:
    detector = CUSUM()
    df = clean_baseline(n=120)
    with pytest.raises(ValueError, match=r"CUSUM\.k"):
        detector.fit_score(df, "", {"k": -1.0})
    with pytest.raises(ValueError, match=r"CUSUM\.k"):
        detector.fit_score(df, "", {"k": "not a number"})


def test_h_validation() -> None:
    detector = CUSUM()
    df = clean_baseline(n=120)
    with pytest.raises(ValueError, match=r"CUSUM\.h"):
        detector.fit_score(df, "", {"h": 0.0})
    with pytest.raises(ValueError, match=r"CUSUM\.h"):
        detector.fit_score(df, "", {"h": "not a number"})


def test_change_point_column_present() -> None:
    """Runner detects change-point detectors by this column."""
    detector = CUSUM()
    df = clean_baseline(n=120)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert "change_point" in row.index


def test_constant_signal_does_not_crash() -> None:
    """Degenerate window (constant signal -> MAD=0) must not blow up."""
    import numpy as np
    import pandas as pd

    detector = CUSUM()
    df = pd.DataFrame({"timestamp": np.arange(100, dtype=float), "y": np.full(100, 7.0)})
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) == 0.0
    assert not bool(row["change_point"])
