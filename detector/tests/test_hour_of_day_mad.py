"""HourOfDayMAD detector tests."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from promanomaly.detectors.hour_of_day_mad import HourOfDayMAD


def _diurnal_history(
    *,
    days: int = 7,
    step_seconds: float = 300.0,
    amplitude: float = 5.0,
    noise: float = 0.1,
    spike: float = 0.0,
    seed: int = 0,
) -> pd.DataFrame:
    """Build a multi-day diurnal time-series anchored at midnight UTC.

    The mean follows a sin wave with one full cycle per day so each
    hour-of-day bucket has a distinct expected value. When ``spike`` is
    non-zero it is added to the very last sample only.
    """
    rng = np.random.default_rng(seed)
    n = int(days * 86400 / step_seconds)
    # Anchor to midnight UTC so bucket indices are predictable in tests.
    base_ts = 1_700_000_000.0 - (1_700_000_000.0 % 86400.0)
    timestamps = base_ts + np.arange(n) * step_seconds
    seconds_in_day = timestamps % 86400.0
    base_signal = amplitude * np.sin(2.0 * np.pi * seconds_in_day / 86400.0)
    values = base_signal + rng.normal(0.0, noise, size=n)
    if spike:
        values[-1] += spike
    return pd.DataFrame({"timestamp": timestamps, "y": values})


def test_metadata() -> None:
    detector = HourOfDayMAD()
    assert detector.name == "HourOfDayMAD"
    # v0.5 added the sliding-window opt-in alongside the existing lookback /
    # bucket_width knobs — same shape DayOfWeekMAD ships with.
    assert detector.defaults == {
        "lookback": "7d",
        "bucket_width": "1h",
        "baseline_refresh_interval": "1h",
    }
    assert "bucket" in detector.description.lower()


def test_baseline_refresh_interval_opt_in() -> None:
    # The runner's sliding-window strategy keys off this classmethod;
    # HourOfDayMAD opts in with a 1h default so the 7d lookback doesn't
    # re-fetch on every refresh.
    assert HourOfDayMAD.baseline_refresh_interval_seconds({}) == pytest.approx(3600.0)
    assert HourOfDayMAD.baseline_refresh_interval_seconds(
        {"baseline_refresh_interval": "30m"}
    ) == pytest.approx(1800.0)


def test_query_window_seconds_honours_lookback() -> None:
    # The runner uses this hook to expand the per-query window when a
    # stratified detector is configured in the same query as MAD/Hampel.
    seven_days = HourOfDayMAD.query_window_seconds({"lookback": "7d"})
    one_day = HourOfDayMAD.query_window_seconds({"lookback": "1d"})
    assert seven_days == pytest.approx(7 * 86400.0)
    assert one_day == pytest.approx(86400.0)


def test_clean_diurnal_scores_low() -> None:
    detector = HourOfDayMAD()
    df = _diurnal_history()
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) < 3.0


def test_off_bucket_spike_scores_high() -> None:
    detector = HourOfDayMAD()
    df = _diurnal_history(spike=20.0)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) > 3.0


def test_bucket_width_must_divide_day() -> None:
    detector = HourOfDayMAD()
    df = _diurnal_history()
    with pytest.raises(ValueError):
        detector.fit_score(df, "", {"bucket_width": "7m"})


def test_empty_same_bucket_history_returns_zero_score() -> None:
    # Only a single sample at one bucket — no history to compare against
    # for that bucket, so the score is the documented zero rather than
    # NaN or an exception.
    detector = HourOfDayMAD()
    df = pd.DataFrame({"timestamp": [1_700_000_000.0], "y": [42.0]})
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) == 0.0
