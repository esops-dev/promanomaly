"""Anomaly recurrence pattern detection.

Computes a stratified firing-rate over day-of-week x hour-of-day buckets
by querying the detector's own emitted ``anomaly_outside_threshold``
history from the TSDB.  The result is a 7x24 heatmap of how often a
given ``(id, group)`` series has been anomalous at each (day, hour)
combination — an answer to "is this a recurring pattern?" without any
persisted state.

The analysis is opt-in per group via ``recurrence.enabled: true`` in the
group config.
"""

from __future__ import annotations

import datetime
import math
from typing import TYPE_CHECKING

from .state.snapshot import Sample

if TYPE_CHECKING:
    from .source import QuerySeries

# Day-of-week names used as label values (ISO 8601: Monday=0).
_DOW_NAMES = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def compute_recurrence_samples(
    series_list: list[QuerySeries],
    *,
    group: str,
) -> list[Sample]:
    """Turn historical outside-threshold series into recurrence-score samples.

    For each distinct ``(id, group)`` identity in *series_list*, the
    rolling-window samples are bucketed by (day_of_week, hour_of_day) in
    UTC, and the firing rate (fraction of samples where value >= 0.5) per
    bucket is emitted as ``anomaly_recurrence_score``.

    When multiple detectors score the same ``id``, the TSDB returns one
    ``anomaly_outside_threshold`` series per detector. This function
    deduplicates by ``(id, timestamp)`` — at each point in time, it takes
    the max value across detectors (any-detector-fires semantics), so the
    recurrence rate reflects how often the *series* was anomalous, not how
    many detectors agreed.

    Only buckets with at least one observation are emitted so cardinality
    stays bounded at ``<= 168 * number_of_ids``.

    Args:
        series_list: Historical ``anomaly_outside_threshold`` series from
            the TSDB.  The caller should use ``max by (id, group)`` in the
            PromQL query when possible; this function handles the residual
            case where the raw per-detector series arrive.
        group: The group name (used to match the ``group`` label).

    Returns:
        A list of :class:`Sample` objects, one per non-empty bucket.
    """
    # Step 1: Deduplicate across detectors. For each (id, timestamp),
    # take the max value so that if *any* detector fires, the point counts
    # as firing — matching the ``max`` ensemble semantics.
    #
    # Key: (series_id, timestamp) -> max(value)
    deduped: dict[tuple[str, float], float] = {}

    for series in series_list:
        series_id = series.labels.get("id", "")
        series_group = series.labels.get("group", "")
        if series_group != group:
            continue

        df = series.samples
        for ts_val, y_val in zip(df["timestamp"], df["y"], strict=True):
            try:
                ts = float(ts_val)
                y = float(y_val)
            except (TypeError, ValueError):
                continue
            if math.isnan(ts) or math.isnan(y):
                continue

            point_key = (series_id, ts)
            prev = deduped.get(point_key)
            if prev is None or y > prev:
                deduped[point_key] = y

    # Step 2: Bucket by (id, dow, hour) and compute firing rates.
    BucketKey = tuple[str, int, int]  # (id, dow, hour)
    firing: dict[BucketKey, int] = {}
    total: dict[BucketKey, int] = {}

    for (series_id, ts), value in deduped.items():
        dt = datetime.datetime.fromtimestamp(ts, tz=datetime.UTC)
        dow = dt.weekday()  # Monday=0
        hour = dt.hour

        key: BucketKey = (series_id, dow, hour)
        total[key] = total.get(key, 0) + 1
        if value >= 0.5:
            firing[key] = firing.get(key, 0) + 1

    out: list[Sample] = []
    for key in sorted(total):
        series_id, dow, hour = key
        t = total[key]
        if t == 0:
            continue
        rate = firing.get(key, 0) / t
        labels = tuple(
            sorted(
                {
                    "id": series_id,
                    "group": group,
                    "day_of_week": _DOW_NAMES[dow],
                    "hour_of_day": str(hour),
                }.items()
            )
        )
        out.append(Sample(metric="anomaly_recurrence_score", labels=labels, value=rate))

    return out


__all__ = ["compute_recurrence_samples"]
