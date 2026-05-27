"""DayOfWeekMAD detector tests.

The detector layers a day-of-week dimension on top of HourOfDayMAD,
so the tests focus on the cells where they behave differently:
weekday vs weekend baselines, multi-week lookback, and the
``baseline_refresh_interval`` opt-in to the runner's sliding-window
fetch strategy.
"""

from __future__ import annotations

import datetime as _dt

import numpy as np
import pandas as pd
import pytest

from promanomaly.config import parse_duration
from promanomaly.detectors.day_of_week_mad import DayOfWeekMAD

# Monday 2024-01-01 00:00 UTC — anchors the synthetic series so the
# bucket arithmetic is predictable across tests.
_MONDAY_MIDNIGHT = _dt.datetime(2024, 1, 1, tzinfo=_dt.UTC).timestamp()


def _weekly_history(
    *,
    weeks: int = 4,
    step_seconds: float = 300.0,
    weekday_amplitude: float = 5.0,
    weekend_amplitude: float = 1.0,
    noise: float = 0.1,
    spike: float = 0.0,
    seed: int = 0,
) -> pd.DataFrame:
    """Synthetic weekly signal: high amplitude on weekdays, low on weekends.

    The base signal is a daily sin wave whose amplitude depends on the
    weekday — the canonical "B2B traffic" shape. When ``spike`` is
    non-zero it is applied to the very last sample only.
    """
    rng = np.random.default_rng(seed)
    n = int(weeks * 7 * 86400 / step_seconds)
    timestamps = _MONDAY_MIDNIGHT + np.arange(n) * step_seconds

    values = np.zeros(n, dtype=float)
    for idx, ts in enumerate(timestamps):
        dt = _dt.datetime.fromtimestamp(float(ts), tz=_dt.UTC)
        seconds_in_day = ts % 86400.0
        is_weekend = dt.weekday() >= 5
        amplitude = weekend_amplitude if is_weekend else weekday_amplitude
        values[idx] = amplitude * np.sin(2.0 * np.pi * seconds_in_day / 86400.0)
    values += rng.normal(0.0, noise, size=n)
    if spike:
        values[-1] += spike
    return pd.DataFrame({"timestamp": timestamps, "y": values})


def test_metadata() -> None:
    detector = DayOfWeekMAD()
    assert detector.name == "DayOfWeekMAD"
    assert detector.defaults == {
        "lookback": "4w",
        "bucket_width": "1h",
        "baseline_refresh_interval": "1h",
    }
    assert "bucket" in detector.description.lower()


def test_query_window_seconds_honours_weekly_lookback() -> None:
    # The runner uses this hook to widen the per-query fetch. A 4w
    # lookback drives a 28-day fetch; 2w drives a 14-day fetch.
    four_weeks = DayOfWeekMAD.query_window_seconds({"lookback": "4w"})
    two_weeks = DayOfWeekMAD.query_window_seconds({"lookback": "2w"})
    assert four_weeks == pytest.approx(4 * 7 * 86400.0)
    assert two_weeks == pytest.approx(2 * 7 * 86400.0)


def test_baseline_refresh_interval_opt_in_for_sliding_window() -> None:
    # The runner's sliding-window strategy keys off this method; missing
    # or None disables the optimisation. DayOfWeekMAD opts in with a
    # 1h default so a long lookback doesn't hammer the TSDB every refresh.
    one_hour = DayOfWeekMAD.baseline_refresh_interval_seconds({})
    custom = DayOfWeekMAD.baseline_refresh_interval_seconds({"baseline_refresh_interval": "15m"})
    assert one_hour == pytest.approx(3600.0)
    assert custom == pytest.approx(900.0)


def test_parse_duration_accepts_weeks_for_stratified_lookback() -> None:
    # The base parser carries the ``w`` unit so DayOfWeekMAD's
    # multi-week lookbacks are spelled ``4w`` rather than ``28d``.
    # Same parser used everywhere else, so any duration field in any
    # detector / config block accepts the same set of units.
    assert parse_duration("4w") == 4 * 7 * 86400.0
    assert parse_duration("3d") == 3 * 86400.0
    assert parse_duration("1h") == 3600.0


def test_clean_weekly_signal_scores_low() -> None:
    detector = DayOfWeekMAD()
    df = _weekly_history()
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) < 3.0


def test_weekday_amplitude_on_weekend_bucket_scores_high() -> None:
    # A weekday-amplitude spike landing on a weekend bucket should
    # score high: weekend baselines have low amplitude, so the latest
    # sample looks unlike its same-bucket history.
    detector = DayOfWeekMAD()
    df = _weekly_history(spike=20.0)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) > 3.0


def test_bucket_width_must_divide_day() -> None:
    detector = DayOfWeekMAD()
    df = _weekly_history()
    with pytest.raises(ValueError):
        detector.fit_score(df, "", {"bucket_width": "7m"})


def test_empty_same_bucket_history_returns_zero_score() -> None:
    detector = DayOfWeekMAD()
    df = pd.DataFrame({"timestamp": [_MONDAY_MIDNIGHT], "y": [42.0]})
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) == 0.0


def test_weekday_baseline_is_separate_from_weekend() -> None:
    # Build a history where the weekday bucket has a tight cluster
    # around 5.0 and the weekend bucket sits near 1.0. A latest sample
    # of 5.5 in the weekday bucket should score low — but a 5.5 sample
    # in the weekend bucket should score high. This separates the
    # DayOfWeekMAD verdict from a single hour-of-day baseline that
    # would median the weekday + weekend samples together.
    rng = np.random.default_rng(1)
    timestamps = []
    values = []
    for week in range(4):
        base = _MONDAY_MIDNIGHT + week * 7 * 86400
        # Tuesday 10:00 (weekday) — value ~ 5.0
        timestamps.append(base + 86400 + 10 * 3600)
        values.append(5.0 + rng.normal(0.0, 0.05))
        # Saturday 10:00 (weekend) — value ~ 1.0
        timestamps.append(base + 5 * 86400 + 10 * 3600)
        values.append(1.0 + rng.normal(0.0, 0.05))
    # Latest sample: Saturday 10:00 with weekday-like value
    timestamps.append(_MONDAY_MIDNIGHT + 4 * 7 * 86400 + 5 * 86400 + 10 * 3600)
    values.append(5.0)
    df = pd.DataFrame({"timestamp": timestamps, "y": values})
    row = DayOfWeekMAD().fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) > 10.0
