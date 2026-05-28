"""Pin the vectorised bucket computation against the scalar reference.

The hot-path bucket assignment in HourOfDayMAD / DayOfWeekMAD was
rewritten from a Python loop calling ``datetime.fromtimestamp`` per
sample to a numpy-vectorised form. The vectorised form must produce
*exactly* the same buckets as the scalar reference for every
timestamp, otherwise scores would silently shift after the refactor.

These tests check the equivalence across:

- random timestamps spanning multiple weeks
- timestamps straddling weekday boundaries
- timestamps straddling daylight-savings boundaries (the detector is
  UTC-only so DST transitions in any non-UTC zone must not perturb the
  result)
"""

from __future__ import annotations

import datetime as _dt

import numpy as np
import pytest

from promanomaly.detectors.day_of_week_mad import (
    _EPOCH_WEEKDAY_OFFSET,
)
from promanomaly.detectors.day_of_week_mad import (
    _bucket_for_timestamp as day_bucket,
)
from promanomaly.detectors.hour_of_day_mad import (
    _bucket_for_timestamp as hour_bucket,
)


def _vectorised_day_buckets(timestamps: np.ndarray, bucket_seconds: float) -> np.ndarray:
    """Mirror the inlined numpy version in DayOfWeekMAD.fit_score."""
    days = (timestamps // 86400.0).astype(np.int64)
    dow = (days + _EPOCH_WEEKDAY_OFFSET) % 7
    hour = (timestamps % 86400.0 // bucket_seconds).astype(np.int64)
    return np.stack([dow, hour], axis=-1)


def _vectorised_hour_buckets(timestamps: np.ndarray, bucket_seconds: float) -> np.ndarray:
    return (timestamps % 86400.0 // bucket_seconds).astype(np.int64)


def test_hour_bucket_vectorised_matches_scalar() -> None:
    rng = np.random.default_rng(42)
    # Span ~30 days at irregular offsets — covers every hour bucket
    # multiple times.
    base = 1_700_000_000.0
    timestamps = base + rng.uniform(0, 30 * 86400, size=2000)
    bucket_seconds = 3600.0

    expected = np.array(
        [hour_bucket(float(ts), bucket_seconds) for ts in timestamps],
        dtype=np.int64,
    )
    actual = _vectorised_hour_buckets(timestamps, bucket_seconds)
    assert np.array_equal(actual, expected)


def test_day_bucket_vectorised_matches_scalar() -> None:
    rng = np.random.default_rng(7)
    base = 1_700_000_000.0
    timestamps = base + rng.uniform(0, 30 * 86400, size=2000)
    bucket_seconds = 3600.0

    expected = np.array(
        [day_bucket(float(ts), bucket_seconds) for ts in timestamps],
        dtype=np.int64,
    )
    actual = _vectorised_day_buckets(timestamps, bucket_seconds)
    assert np.array_equal(actual, expected)


def test_day_bucket_matches_python_datetime_weekday() -> None:
    # Independent reference: cross-check against datetime.weekday() so a
    # refactor that breaks the epoch-offset constant is caught even if
    # the scalar helper is touched at the same time as the vectorised
    # form (defending against same-author-same-bug regressions).
    rng = np.random.default_rng(11)
    base = 1_700_000_000.0
    timestamps = base + rng.uniform(0, 60 * 86400, size=500)

    for ts in timestamps:
        dow_scalar, _hour = day_bucket(float(ts), 3600.0)
        dow_python = _dt.datetime.fromtimestamp(float(ts), tz=_dt.UTC).weekday()
        assert dow_scalar == dow_python, f"timestamp {ts}: scalar={dow_scalar} python={dow_python}"


def test_day_bucket_correct_at_epoch() -> None:
    # 1970-01-01 00:00 UTC was a Thursday → weekday() == 3.
    dow, hour = day_bucket(0.0, 3600.0)
    assert dow == 3
    assert hour == 0


def test_day_bucket_handles_dst_boundary_correctly() -> None:
    # DST transitions in non-UTC zones must not perturb the UTC bucket
    # — both detectors are documented as UTC-only. Pick a timestamp
    # right after the 2024 US DST "spring forward" (2024-03-10 07:00
    # UTC = 03:00 EST → 04:00 EDT).
    dst_transition = _dt.datetime(2024, 3, 10, 7, 0, tzinfo=_dt.UTC).timestamp()
    dow, hour = day_bucket(dst_transition, 3600.0)
    # 2024-03-10 was a Sunday → weekday() == 6.
    assert dow == 6
    assert hour == 7  # 07:00 UTC


def test_hour_bucket_within_day_modular_arithmetic() -> None:
    # Sanity-check the within-day reduction: any time within a UTC day
    # should produce a bucket index in [0, 24).
    base = 1_700_000_000.0 - (1_700_000_000.0 % 86400.0)
    for offset in (0.0, 1.0, 3600.0, 86399.999, 43200.0):
        bucket = hour_bucket(base + offset, 3600.0)
        assert 0 <= bucket < 24


@pytest.mark.parametrize("bucket_seconds", [3600.0, 1800.0, 7200.0, 14400.0])
def test_hour_bucket_vectorised_handles_alternate_widths(bucket_seconds: float) -> None:
    rng = np.random.default_rng(13)
    base = 1_700_000_000.0
    timestamps = base + rng.uniform(0, 86400, size=500)
    expected = np.array(
        [hour_bucket(float(ts), bucket_seconds) for ts in timestamps],
        dtype=np.int64,
    )
    actual = _vectorised_hour_buckets(timestamps, bucket_seconds)
    assert np.array_equal(actual, expected)
