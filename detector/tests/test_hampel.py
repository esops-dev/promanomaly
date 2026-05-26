"""Hampel detector tests."""

from __future__ import annotations

import pytest

from promanomaly.detectors.hampel import Hampel
from tests.fixtures import (
    clean_baseline,
    gradual_drift,
    point_spike,
)


def test_hampel_metadata() -> None:
    detector = Hampel()
    assert detector.name == "Hampel"
    assert detector.defaults == {"t0": 3}


def test_clean_baseline_scores_low() -> None:
    detector = Hampel()
    df = clean_baseline()
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) < 3.0


def test_point_spike_scores_high() -> None:
    detector = Hampel()
    df = point_spike()
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) > 3.0


def test_drifting_baseline_keeps_recent_score_low() -> None:
    # Hampel's selling point vs. plain MAD: when the baseline drifts
    # smoothly within the window the *local* median tracks the drift, so
    # the latest in-trend sample should not look anomalous.
    detector = Hampel()
    df = gradual_drift(slope=0.05, noise=0.05)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) < 3.0


def test_invalid_t0_raises() -> None:
    detector = Hampel()
    df = clean_baseline()
    with pytest.raises(ValueError):
        detector.fit_score(df, "", {"t0": "not-a-number"})


def test_t0_param_is_honoured() -> None:
    # Sanity check: explicit t0 reaches the detector and changes the
    # computed baseline / score (we don't pin the direction — the
    # drift fixture is too synthetic for a robust ordering claim).
    detector = Hampel()
    df = gradual_drift(slope=0.1, noise=0.01)
    small = detector.fit_score(df, "", {"t0": 2}).iloc[-1]
    large = detector.fit_score(df, "", {"t0": 60}).iloc[-1]
    assert float(small["baseline"]) != float(large["baseline"])
