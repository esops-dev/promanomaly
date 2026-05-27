"""Day-of-week stratified MAD detector.

The full stratified-baseline carve-out. Extends HourOfDayMAD by adding a
day-of-week dimension so weekly-periodic signals don't pollute their own
baselines. The canonical case: B2B traffic is quiet on weekends, batch
jobs only run on Mondays, business-hours error rates differ from
overnight ones. A single (hour-of-day) MAD smears these together; the
7x24 grid keeps them separate.

Stratified does NOT mean forecasting. The detector scores the present
against historical observations of the same (day-of-week, hour-of-day)
bucket; it never predicts future values. The shared sliding-window
fetch strategy (short window each refresh, long baseline window every
``baseline_refresh_interval``) keeps the multi-week lookback affordable
on the TSDB — see ``docs/scaling-stratified.md``.
"""

from __future__ import annotations

import math
from typing import Any, ClassVar

import numpy as np
import pandas as pd

from ..config import parse_duration
from .base import ParamSpec

# Unix epoch (1970-01-01) was a Thursday. Python's ``datetime.weekday()``
# returns 0 for Monday, so a Thursday at the epoch is weekday=3 — add 3
# to ``days_since_epoch`` before reducing mod 7 to get the same value
# without paying for ``datetime.fromtimestamp`` per sample.
_EPOCH_WEEKDAY_OFFSET = 3


def _bucket_for_timestamp(ts: float, bucket_width_seconds: float) -> tuple[int, int]:
    """Map a unix timestamp to its ``(day_of_week, hour_bucket)`` cell.

    ``day_of_week`` follows Python's ``datetime.weekday()`` convention
    (Monday == 0). ``hour_bucket`` is the index inside a 24h grid sized
    by ``bucket_width_seconds``. We bucket on UTC because Prometheus
    stores UTC and operators read UTC in Grafana. Time-zone handling is
    deferred until operators actually ask for it — see the architecture
    note in the project guide.

    Kept around as the single-timestamp scalar form for callers that
    need just the latest sample's bucket. The hot path in ``fit_score``
    uses the vectorised numpy equivalent inline.
    """
    days_since_epoch = int(ts // 86400.0)
    day = (days_since_epoch + _EPOCH_WEEKDAY_OFFSET) % 7
    seconds_in_day = ts % 86400.0
    hour_bucket = int(seconds_in_day // bucket_width_seconds)
    return day, hour_bucket


class DayOfWeekMAD:
    """Stratified MAD detector on a 7-day x N-bucket grid.

    Parameters (config ``params:``)
    -------------------------------
    ``lookback`` (duration, default ``4w``)
        Multi-week window of historical samples used to populate the
        per-bucket baselines. Sized in weeks so every weekday bucket has
        several observations to median over. The runner widens the
        per-query fetch to honour this even when other detectors in the
        same query only need a short rolling window.
    ``bucket_width`` (duration, default ``1h``)
        Width of each within-day bucket. ``1h`` gives the canonical 7x24
        grid. Smaller widths increase resolution but reduce per-bucket
        sample count.
    ``baseline_refresh_interval`` (duration, default ``1h``)
        Cadence at which the multi-week baseline is re-pulled from the
        TSDB. The latest sample is still fetched every
        ``server.refresh_interval`` via a short range query; only the
        long baseline obeys this slower cadence. Lowering this trades
        TSDB load for fresher baselines.

    Threshold sensitivity is set via ``alert_thresholds.score``.
    """

    name: ClassVar[str] = "DayOfWeekMAD"
    description: ClassVar[str] = (
        "Stratified MAD baseline by (day-of-week, hour-of-day) bucket. "
        "Pulls a multi-week history and scores the latest sample against "
        "prior samples from the same weekly bucket. Catches weekly "
        "anomalies a single hour-of-day baseline smears away."
    )
    defaults: ClassVar[dict[str, Any]] = {
        "lookback": "4w",
        "bucket_width": "1h",
        "baseline_refresh_interval": "1h",
    }
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="lookback",
            type_="duration",
            default="4w",
            doc=(
                "Multi-week window of baseline samples. Sized in weeks so "
                "every (day-of-week, hour) bucket has several observations."
            ),
        ),
        ParamSpec(
            name="bucket_width",
            type_="duration",
            default="1h",
            doc="Width of each within-day bucket; must divide 24h evenly.",
        ),
        ParamSpec(
            name="baseline_refresh_interval",
            type_="duration",
            default="1h",
            doc=(
                "Cadence at which the long baseline is re-pulled from the "
                "TSDB; the latest sample is still fetched every refresh."
            ),
        ),
    )

    @classmethod
    def query_window_seconds(cls, params: dict[str, Any]) -> float | None:
        """Inform the runner this detector needs a multi-week query window."""
        lookback = params.get("lookback", cls.defaults["lookback"])
        try:
            return float(parse_duration(lookback))
        except (TypeError, ValueError):
            return None

    @classmethod
    def baseline_refresh_interval_seconds(cls, params: dict[str, Any]) -> float | None:
        """Cadence at which the multi-week baseline fetch may be reused.

        Returning a value here opts the detector into the runner's
        sliding-window strategy: the heavy long-history query runs once
        per ``baseline_refresh_interval``, while every refresh still
        fetches the latest short window and stitches it onto the cached
        baseline before scoring. Detectors that return ``None`` (or omit
        the method) keep the per-run full-window fetch behaviour.
        """
        raw = params.get("baseline_refresh_interval", cls.defaults["baseline_refresh_interval"])
        try:
            return float(parse_duration(raw))
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
                f"DayOfWeekMAD.bucket_width must be a duration, got {bucket_raw!r}"
            ) from exc
        if bucket_seconds <= 0:
            raise ValueError(f"DayOfWeekMAD.bucket_width must be positive, got {bucket_seconds}")
        if not math.isclose(86400.0 % bucket_seconds, 0.0, abs_tol=1e-6) and not math.isclose(
            86400.0 % bucket_seconds, bucket_seconds, abs_tol=1e-6
        ):
            raise ValueError(f"DayOfWeekMAD.bucket_width ({bucket_raw}) must divide 24h evenly")

        timestamps = df["timestamp"].to_numpy(dtype=float, copy=False)
        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(timestamps[-1])
        latest_y = float(values[-1])

        latest_bucket = _bucket_for_timestamp(latest_ts, bucket_seconds)

        # Build the same-bucket mask. Exclude the latest sample itself so
        # we score it against history, never against itself.
        history_ts = timestamps[:-1]
        history_values = values[:-1]
        if history_ts.size == 0:
            return _zero_score_row(latest_ts, float(np.median(values)))

        # Vectorised bucket assignment. A 4w * 15s-step series is
        # ~161k samples and the equivalent Python + datetime loop was
        # the dominant cost in this detector before vectorisation.
        # ``(days + 3) % 7`` mirrors ``datetime.weekday()`` because the
        # unix epoch was a Thursday — pinned by
        # ``tests/test_stratified_bucket_vectorisation.py``.
        days_since_epoch = (history_ts // 86400.0).astype(np.int64)
        history_dow = (days_since_epoch + _EPOCH_WEEKDAY_OFFSET) % 7
        history_hour = (history_ts % 86400.0 // bucket_seconds).astype(np.int64)
        mask = (history_dow == latest_bucket[0]) & (history_hour == latest_bucket[1])
        same_bucket_values = history_values[mask]

        if same_bucket_values.size == 0:
            # Sparse-bucket fallback: not enough history for this cell
            # yet (first Tuesday-3am after deployment). Score zero rather
            # than fabricating one — the warmup / confidence path is the
            # right place to surface "we don't have enough data".
            return _zero_score_row(latest_ts, float(np.median(values)))

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


def _zero_score_row(latest_ts: float, baseline: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "timestamp": latest_ts,
                "score": 0.0,
                "baseline": baseline,
                "is_outside": False,
            }
        ]
    )


__all__ = ["DayOfWeekMAD"]
