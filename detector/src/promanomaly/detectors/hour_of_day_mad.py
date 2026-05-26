"""Hour-of-day stratified MAD detector.

The lightweight stratified-baseline carve-out. Where MAD/Hampel pull a
short rolling window of recent data ("is this point anomalous given the
last hour?"), HourOfDayMAD pulls a long history and asks "is this point
anomalous given prior samples from the same hour of day?". This catches
diurnal anomalies that a fixed-window MAD smears away — Tuesday 3am
looking unlike prior 3am samples, even though it might be perfectly
normal compared to the rolling-hour median.

Stratified does NOT mean forecasting. The detector scores the present
against historical observations of the same bucket; it never predicts
future values. The full day-of-week / multi-week version ships later.
"""

from __future__ import annotations

import math
from typing import Any, ClassVar

import numpy as np
import pandas as pd

from ..config import parse_duration
from .base import ParamSpec


def _bucket_for_timestamp(ts: float, bucket_width_seconds: float) -> int:
    """Map a unix timestamp to its bucket index inside a 24h grid.

    We bucket on wall-clock UTC because that's what Prometheus stores
    and what operators see in Grafana. Time-zone awareness is left to
    the next milestone — current users either run in UTC clusters or
    accept a 24h grid shifted from local time.
    """
    seconds_in_day = ts % 86400.0
    return int(seconds_in_day // bucket_width_seconds)


class HourOfDayMAD:
    """Stratified MAD detector on a 24-bucket hour-of-day grid.

    Parameters (config ``params:``)
    -------------------------------
    ``lookback`` (duration, default ``7d``)
        How far back to pull samples for baseline computation. The
        runner expands the query window to honour this — listing
        HourOfDayMAD in a query forces a long-history fetch even when
        the group's ``defaults.window`` is short.
    ``bucket_width`` (duration, default ``1h``)
        Width of each bucket. ``1h`` gives 24 buckets across a day;
        smaller values give finer stratification at the cost of more
        per-bucket sample sparsity.

    Threshold sensitivity is set via ``alert_thresholds.score``.
    """

    name: ClassVar[str] = "HourOfDayMAD"
    description: ClassVar[str] = (
        "Stratified MAD baseline by hour-of-day bucket. Pulls a long history "
        "and scores the latest sample against prior samples from the same "
        "bucket. Catches diurnal anomalies a fixed-window MAD smears away."
    )
    defaults: ClassVar[dict[str, Any]] = {"lookback": "7d", "bucket_width": "1h"}
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="lookback",
            type_="duration",
            default="7d",
            doc="How far back to pull baseline samples (Prometheus duration string).",
        ),
        ParamSpec(
            name="bucket_width",
            type_="duration",
            default="1h",
            doc="Width of each hour-of-day bucket; must divide 24h evenly.",
        ),
    )

    @classmethod
    def query_window_seconds(cls, params: dict[str, Any]) -> float | None:
        """Inform the runner this detector needs a longer query window.

        Returning a value here makes the runner fetch
        ``max(group_default, max_detector_window)`` for the query the
        detector is configured in, so HourOfDayMAD coexists with the
        short-window detectors without forcing the group's default to
        change.
        """
        lookback = params.get("lookback", cls.defaults["lookback"])
        try:
            return float(parse_duration(lookback))
        except (TypeError, ValueError):
            return None

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        del freq
        bucket_raw = params.get("bucket_width", self.defaults["bucket_width"])
        try:
            bucket_seconds = float(parse_duration(bucket_raw))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"HourOfDayMAD.bucket_width must be a duration, got {bucket_raw!r}"
            ) from exc
        if bucket_seconds <= 0:
            raise ValueError(f"HourOfDayMAD.bucket_width must be positive, got {bucket_seconds}")
        # Buckets only make sense as a divisor of a 24h day; reject
        # non-divisors so the bucket grid is unambiguous.
        if not math.isclose(86400.0 % bucket_seconds, 0.0, abs_tol=1e-6) and not math.isclose(
            86400.0 % bucket_seconds, bucket_seconds, abs_tol=1e-6
        ):
            raise ValueError(f"HourOfDayMAD.bucket_width ({bucket_raw}) must divide 24h evenly")

        timestamps = df["timestamp"].to_numpy(dtype=float, copy=False)
        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(timestamps[-1])
        latest_y = float(values[-1])

        latest_bucket = _bucket_for_timestamp(latest_ts, bucket_seconds)

        # Build the same-bucket mask. Exclude the latest sample from
        # the bucket so we score it against its history, not itself.
        buckets = np.array(
            [_bucket_for_timestamp(float(ts), bucket_seconds) for ts in timestamps[:-1]],
            dtype=int,
        )
        same_bucket_values = values[:-1][buckets == latest_bucket]

        if same_bucket_values.size == 0:
            # Sparse-bucket fallback: not enough history for this bucket
            # yet. Emit a zero score — the warmup_policy and confidence
            # gauge (v0.2) are the right places to surface "we don't
            # have enough data for this bucket"; the score itself stays
            # silent rather than fabricating one.
            return pd.DataFrame(
                [
                    {
                        "timestamp": latest_ts,
                        "score": 0.0,
                        "baseline": float(np.median(values)),
                        "is_outside": False,
                    }
                ]
            )

        bucket_median = float(np.median(same_bucket_values))
        bucket_mad = float(np.median(np.abs(same_bucket_values - bucket_median)))
        score = 0.0 if bucket_mad == 0.0 else abs(latest_y - bucket_median) / bucket_mad

        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": score,
                    "baseline": bucket_median,
                    "is_outside": False,
                }
            ]
        )
