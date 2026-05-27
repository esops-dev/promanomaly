"""Safe (non-executing) serialisation for cache payloads.

Wire format
-----------
We need to serialise two kinds of value across Redis:

- :class:`promanomaly.source.QueryResult` for the query cache.
- :class:`promanomaly.state.GroupSnapshot` for the shared snapshot
  cache used by followers in HA mode.

The earlier implementation used :mod:`pickle` for both. Pickle is the
obvious choice (zero schema work, handles pandas DataFrames natively)
but ``pickle.loads`` executes arbitrary Python — an attacker who can
write to Redis gets RCE inside every promanomaly pod that reads from
it. For the trusted-Redis-in-same-VPC case that is defensible, but
the per-cache cost of dropping pickle is one small encoder; the
security upside is large enough to justify it.

Both encoders produce UTF-8 JSON bytes and tag the payload with a
``v`` (version) field so future format migrations can branch without
ambiguity. Decode errors return ``None`` — the caller treats that as
a corrupt entry and refetches.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from .source import QueryResult, QuerySeries
from .state import GroupSnapshot, Sample

# Bump only on incompatible wire-format changes. Forward-compatible
# additions (new optional fields) can stay on v1.
_QUERY_RESULT_VERSION = 1
_SNAPSHOT_VERSION = 1


# ----------------------------------------------------------------------
# QueryResult
# ----------------------------------------------------------------------


def encode_query_result(value: QueryResult) -> bytes:
    payload = {
        "v": _QUERY_RESULT_VERSION,
        "truncated": bool(value.truncated),
        "series": [_encode_series(series) for series in value.series],
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def decode_query_result(blob: bytes) -> QueryResult | None:
    try:
        payload = json.loads(blob)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get("v") != _QUERY_RESULT_VERSION:
        return None
    raw_series = payload.get("series")
    if not isinstance(raw_series, list):
        return None
    series_list: list[QuerySeries] = []
    for raw in raw_series:
        series = _decode_series(raw)
        if series is None:
            return None
        series_list.append(series)
    return QueryResult(series=series_list, truncated=bool(payload.get("truncated", False)))


def _encode_series(series: QuerySeries) -> dict[str, Any]:
    samples = series.samples
    # Column-oriented arrays keep the JSON small and avoid one dict
    # per row. The DataFrame we round-trip back to has identical column
    # names and dtypes (always float64 for ``timestamp`` / ``y``).
    return {
        "labels": dict(series.labels),
        "ts": [float(v) for v in samples["timestamp"].tolist()],
        "y": [float(v) for v in samples["y"].tolist()],
    }


def _decode_series(raw: Any) -> QuerySeries | None:
    if not isinstance(raw, dict):
        return None
    labels = raw.get("labels")
    ts = raw.get("ts")
    y = raw.get("y")
    if not isinstance(labels, dict) or not isinstance(ts, list) or not isinstance(y, list):
        return None
    if len(ts) != len(y):
        return None
    try:
        frame = pd.DataFrame(
            {
                "timestamp": pd.array([float(t) for t in ts], dtype="float64"),
                "y": pd.array([float(v) for v in y], dtype="float64"),
            }
        )
    except (TypeError, ValueError):
        return None
    label_dict: dict[str, str] = {}
    for k, v in labels.items():
        if not isinstance(k, str) or not isinstance(v, str):
            return None
        label_dict[k] = v
    return QuerySeries(labels=label_dict, samples=frame)


# ----------------------------------------------------------------------
# GroupSnapshot
# ----------------------------------------------------------------------


def encode_snapshot(value: GroupSnapshot) -> bytes:
    payload = {
        "v": _SNAPSHOT_VERSION,
        "group": value.group,
        "timestamp": float(value.timestamp),
        "samples": [_encode_sample(s) for s in value.samples],
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def decode_snapshot(blob: bytes) -> GroupSnapshot | None:
    try:
        payload = json.loads(blob)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get("v") != _SNAPSHOT_VERSION:
        return None
    group = payload.get("group")
    timestamp = payload.get("timestamp")
    raw_samples = payload.get("samples")
    if not isinstance(group, str) or not isinstance(raw_samples, list):
        return None
    try:
        ts_value = float(timestamp)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    samples: list[Sample] = []
    for raw in raw_samples:
        sample = _decode_sample(raw)
        if sample is None:
            return None
        samples.append(sample)
    return GroupSnapshot(group=group, timestamp=ts_value, samples=samples)


def _encode_sample(sample: Sample) -> dict[str, Any]:
    return {
        "metric": sample.metric,
        # Labels round-trip as a flat list of [k, v] pairs so the encoder
        # preserves order (matching the producer's sorted form).
        "labels": [list(pair) for pair in sample.labels],
        "value": float(sample.value),
    }


def _decode_sample(raw: Any) -> Sample | None:
    if not isinstance(raw, dict):
        return None
    metric = raw.get("metric")
    raw_labels = raw.get("labels")
    value = raw.get("value")
    if not isinstance(metric, str) or not isinstance(raw_labels, list):
        return None
    label_pairs: list[tuple[str, str]] = []
    for pair in raw_labels:
        if (
            not isinstance(pair, list)
            or len(pair) != 2
            or not isinstance(pair[0], str)
            or not isinstance(pair[1], str)
        ):
            return None
        label_pairs.append((pair[0], pair[1]))
    try:
        value_f = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return Sample(metric=metric, labels=tuple(label_pairs), value=value_f)


__all__ = [
    "decode_query_result",
    "decode_snapshot",
    "encode_query_result",
    "encode_snapshot",
]
