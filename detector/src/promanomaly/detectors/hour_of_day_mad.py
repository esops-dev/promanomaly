"""Hour-of-day stratified MAD detector.

The lightweight stratified-baseline carve-out. Where MAD/Hampel pull a
short rolling window of recent data ("is this point anomalous given the
last hour?"), HourOfDayMAD pulls a long history and asks "is this point
anomalous given prior samples from the same hour of day?". This catches
diurnal anomalies that a fixed-window MAD smears away — Tuesday 3am
looking unlike prior 3am samples, even though it might be perfectly
normal compared to the rolling-hour median.

See :mod:`promanomaly.detectors.stratified_base` for the shared scoring
pipeline; this module supplies the 1D-bucket coordinate computation.
"""

from __future__ import annotations

from typing import Any, ClassVar

import numpy as np

from .base import ParamSpec
from .stratified_base import StratifiedMADBase


def _bucket_for_timestamp(ts: float, bucket_width_seconds: float) -> int:
    """Map a unix timestamp to its bucket index inside a 24h grid.

    We bucket on wall-clock UTC because that's what Prometheus stores
    and what operators see in Grafana. Time-zone awareness is left to
    the next milestone — current users either run in UTC clusters or
    accept a 24h grid shifted from local time.
    """
    seconds_in_day = ts % 86400.0
    return int(seconds_in_day // bucket_width_seconds)


class HourOfDayMAD(StratifiedMADBase):
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
    ``baseline_refresh_interval`` (duration, default ``1h``)
        Cadence at which the long baseline window is re-pulled from the
        TSDB. The latest sample is still fetched every
        ``server.refresh_interval`` via a short range query; only the
        long baseline obeys this slower cadence. Opts the detector into
        the runner's sliding-window fetch strategy.

    Threshold sensitivity is set via ``alert_thresholds.score``.
    """

    name: ClassVar[str] = "HourOfDayMAD"
    description: ClassVar[str] = (
        "Stratified MAD baseline by hour-of-day bucket. Pulls a long history "
        "and scores the latest sample against prior samples from the same "
        "bucket. Catches diurnal anomalies a fixed-window MAD smears away."
    )
    defaults: ClassVar[dict[str, Any]] = {
        "lookback": "7d",
        "bucket_width": "1h",
        "baseline_refresh_interval": "1h",
    }
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

    def _history_bucket_codes(
        self, history_ts: np.ndarray[Any, Any], bucket_seconds: float
    ) -> np.ndarray[Any, Any]:
        # Vectorised: a 7d * 15s-step series is ~40k samples and the
        # equivalent Python loop dominated detector compute time
        # before vectorisation. The integer division mirrors
        # ``_bucket_for_timestamp`` element-for-element — pinned by
        # ``tests/test_stratified_bucket_vectorisation.py``.
        return (history_ts % 86400.0 // bucket_seconds).astype(np.int64)

    def _latest_bucket_code(self, latest_ts: float, bucket_seconds: float) -> int:
        return _bucket_for_timestamp(latest_ts, bucket_seconds)


__all__ = ["HourOfDayMAD"]
