"""BOCPD detector tests against the synthetic-series fixtures."""

from __future__ import annotations

import pytest

from promanomaly.detectors.bocpd import BOCPD
from tests.fixtures import (
    clean_baseline,
    point_spike,
    step_change,
)


def test_bocpd_metadata() -> None:
    detector = BOCPD()
    assert detector.name == "BOCPD"
    assert "bayesian" in detector.description.lower()
    # ``threshold`` default is the per-detector override the runner picks
    # up; documented in change-points.md and surfaced in /debug/detectors.
    assert detector.defaults["threshold"] == 0.5
    assert detector.defaults["hazard"] == 250


def test_clean_baseline_does_not_fire() -> None:
    detector = BOCPD()
    df = clean_baseline(n=200, mean=10.0, noise=0.1)
    row = detector.fit_score(df, "", {}).iloc[-1]
    # On a clean baseline the posterior concentrates at long run lengths,
    # so the recent-run-length mass stays near the prior hazard rate.
    assert float(row["score"]) < 0.2
    assert not bool(row["change_point"])
    assert not bool(row["is_outside"])


def test_recent_step_change_fires() -> None:
    detector = BOCPD()
    # Change-point lands at the latest sample — score should jump to ~1.
    df = step_change(n=200, step=5.0, noise=0.1, change_at=199)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) >= 0.5
    assert bool(row["change_point"])
    assert bool(row["is_outside"])


def test_change_point_within_lag_fires() -> None:
    # CP at sample 195, latest is 199, default lag=15 — well inside.
    detector = BOCPD()
    df = step_change(n=200, step=5.0, noise=0.1, change_at=195)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) >= 0.5
    assert bool(row["change_point"])


def test_old_change_point_does_not_fire() -> None:
    # CP at sample 100, well outside the default lag of 15. The posterior
    # has moved to a long run-length and the recent-run mass is near
    # the prior hazard rate.
    detector = BOCPD()
    df = step_change(n=200, step=5.0, noise=0.1, change_at=100)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) < 0.5
    assert not bool(row["change_point"])


def test_threshold_param_controls_firing() -> None:
    """A tighter threshold suppresses borderline fires; looser fires more."""
    detector = BOCPD()
    df = step_change(n=200, step=5.0, noise=0.1, change_at=199)
    loose = detector.fit_score(df, "", {"threshold": 0.1}).iloc[-1]
    tight = detector.fit_score(df, "", {"threshold": 0.99}).iloc[-1]
    # Same input -> same raw score; only the verdict differs.
    assert float(loose["score"]) == float(tight["score"])
    assert bool(loose["change_point"])
    # When the score is exactly 1.0 even the tightest threshold fires;
    # but the test guards the OTHER direction: when raw score is high.
    assert float(tight["score"]) >= 0.99 or not bool(tight["change_point"])


def test_score_is_a_probability() -> None:
    detector = BOCPD()
    df = step_change(n=200, step=5.0, noise=0.1, change_at=199)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert 0.0 <= float(row["score"]) <= 1.0


def test_point_spike_fires() -> None:
    # A single late spike looks like a change-point to BOCPD because the
    # spike doesn't fit the established regime and the posterior mass
    # shifts to fresh run-lengths.
    detector = BOCPD()
    df = point_spike(n=200, spike=10.0, noise=0.1)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) >= 0.5


def test_hazard_validation() -> None:
    detector = BOCPD()
    df = clean_baseline(n=120)
    with pytest.raises(ValueError, match="hazard"):
        detector.fit_score(df, "", {"hazard": 0})
    with pytest.raises(ValueError, match="hazard"):
        detector.fit_score(df, "", {"hazard": "not a number"})


def test_threshold_validation() -> None:
    detector = BOCPD()
    df = clean_baseline(n=120)
    with pytest.raises(ValueError, match="threshold"):
        detector.fit_score(df, "", {"threshold": "not a number"})


def test_change_point_column_present() -> None:
    """The runner detects change-point detectors by the presence of the
    ``change_point`` column in the score row — assert BOCPD emits it
    even when the latest sample is not a fire."""
    detector = BOCPD()
    df = clean_baseline(n=120)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert "change_point" in row.index


def test_short_window_does_not_saturate() -> None:
    """A window no longer than ``lag`` must not fire a spurious change-point.

    The score is the posterior mass at run lengths <= lag; when the window
    has <= lag samples every run length is "recent", so without a guard the
    cumulative mass is trivially ~1.0 and fires on any signal. A clean
    baseline that short must report no change-point.
    """
    detector = BOCPD()
    df = clean_baseline(n=12, mean=10.0, noise=0.1)  # n <= default lag (15)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) == 0.0
    assert not bool(row["change_point"])
    assert not bool(row["is_outside"])
    # The baseline is still a usable estimate of the (single) regime mean.
    assert float(row["baseline"]) == pytest.approx(10.0, abs=1.0)


def test_short_window_guard_respects_lag_param() -> None:
    # With a smaller lag the same window is long enough to leave a tail,
    # so the guard does not suppress scoring.
    detector = BOCPD()
    df = clean_baseline(n=12, mean=10.0, noise=0.1)
    guarded = detector.fit_score(df, "", {"lag": 15}).iloc[-1]
    scored = detector.fit_score(df, "", {"lag": 3}).iloc[-1]
    assert float(guarded["score"]) == 0.0
    # lag=3 leaves run-length states beyond the recent window, so the
    # detector computes a real (here still-low, clean-baseline) score.
    assert float(scored["score"]) < 0.5
