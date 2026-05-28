"""Day-of-week stratified MAD detector.

The full stratified-baseline carve-out. Extends HourOfDayMAD by adding a
day-of-week dimension so weekly-periodic signals don't pollute their own
baselines. The canonical case: B2B traffic is quiet on weekends, batch
jobs only run on Mondays, business-hours error rates differ from
overnight ones. A single (hour-of-day) MAD smears these together; the
7x24 grid keeps them separate.

See :mod:`promanomaly.detectors.stratified_base` for the shared scoring
pipeline; this module supplies the 2D-bucket coordinate computation.
"""

from __future__ import annotations

from typing import Any, ClassVar

import numpy as np

from .base import ParamSpec
from .stratified_base import StratifiedMADBase

# Unix epoch (1970-01-01) was a Thursday. Python's ``datetime.weekday()``
# returns 0 for Monday, so a Thursday at the epoch is weekday=3 — add 3
# to ``days_since_epoch`` before reducing mod 7 to get the same value
# without paying for ``datetime.fromtimestamp`` per sample.
_EPOCH_WEEKDAY_OFFSET = 3

# Safe upper bound on within-day bucket indices used to pack (day, hour)
# into a single int code. ``bucket_width=1s`` gives 86_400 buckets; any
# reasonable bucket_width gives far fewer. Multiplying by ``10**6``
# leaves the int64 well within range and prevents day/hour collisions.
_DAY_CODE_STRIDE = 1_000_000


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


class DayOfWeekMAD(StratifiedMADBase):
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

    def _history_bucket_codes(
        self, history_ts: np.ndarray[Any, Any], bucket_seconds: float
    ) -> np.ndarray[Any, Any]:
        # Vectorised bucket assignment. A 4w * 15s-step series is
        # ~161k samples and the equivalent Python + datetime loop was
        # the dominant cost in this detector before vectorisation.
        # ``(days + 3) % 7`` mirrors ``datetime.weekday()`` because the
        # unix epoch was a Thursday — pinned by
        # ``tests/test_stratified_bucket_vectorisation.py``.
        days_since_epoch = (history_ts // 86400.0).astype(np.int64)
        dow = (days_since_epoch + _EPOCH_WEEKDAY_OFFSET) % 7
        hour = (history_ts % 86400.0 // bucket_seconds).astype(np.int64)
        return dow * _DAY_CODE_STRIDE + hour

    def _latest_bucket_code(self, latest_ts: float, bucket_seconds: float) -> int:
        dow, hour = _bucket_for_timestamp(latest_ts, bucket_seconds)
        return dow * _DAY_CODE_STRIDE + hour


__all__ = ["DayOfWeekMAD"]
