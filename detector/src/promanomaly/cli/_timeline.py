"""Helpers for the ``timeline`` CLI command.

Pure functions that turn fetched anomaly metrics into a time-ordered list of
anomaly events, kept free of I/O so they can be unit-tested directly.

The timeline reads three emitted anomaly metrics over a window:

* ``anomaly_outside_threshold`` — a 0/1 gauge; each 0->1 transition per series
  marks the start of an anomaly.
* ``anomaly_change_point_total`` — a counter; each positive increase between
  consecutive samples marks a change-point firing.
* ``anomaly_severity`` — a gauge whose nearest sample at or before an event is
  attached to that event when available.
"""

from __future__ import annotations

from typing import Any

from ..source import QuerySeries
from ._analyze import _format_iso

__all__ = ["format_timeline_text", "run_timeline"]

# Metric names the timeline understands.
METRIC_OUTSIDE = "anomaly_outside_threshold"
METRIC_CHANGE_POINT = "anomaly_change_point_total"
METRIC_SEVERITY = "anomaly_severity"

# Labels copied onto every event, in display order.
_EVENT_LABELS = ("id", "group", "detector", "detector_instance")


def _identity_key(labels: dict[str, str]) -> tuple[tuple[str, str], ...]:
    """Build a hashable key for matching series across metrics.

    Severity is matched to anomaly/change-point events by the shared
    identifying labels rather than by every label, so a severity series with
    extra labels still lines up with its event source.
    """
    return tuple(sorted((k, labels[k]) for k in _EVENT_LABELS if k in labels))


def _event_labels(labels: dict[str, str]) -> dict[str, str]:
    """Extract the contracted event labels present on a series."""
    return {k: labels[k] for k in _EVENT_LABELS if k in labels}


def _samples(series: QuerySeries) -> list[tuple[float, float]]:
    """Return ``(timestamp, value)`` pairs sorted by timestamp.

    Rows whose timestamp or value is missing are skipped so a gap in the
    underlying data cannot manufacture a spurious transition.
    """
    df = series.samples
    pairs: list[tuple[float, float]] = []
    for ts, y in zip(df["timestamp"], df["y"], strict=True):
        if ts is None or y is None:
            continue
        try:
            fts = float(ts)
            fy = float(y)
        except (TypeError, ValueError):
            continue
        if fts != fts or fy != fy:  # skip NaN
            continue
        pairs.append((fts, fy))
    pairs.sort(key=lambda p: p[0])
    return pairs


class _SeverityLookup:
    """Nearest-at-or-before severity lookup keyed by identifying labels."""

    def __init__(self, series_list: list[QuerySeries]) -> None:
        self._by_key: dict[tuple[tuple[str, str], ...], list[tuple[float, float]]] = {}
        for series in series_list:
            key = _identity_key(series.labels)
            samples = _samples(series)
            if not samples:
                continue
            self._by_key.setdefault(key, []).extend(samples)
        for samples in self._by_key.values():
            samples.sort(key=lambda p: p[0])

    def at(self, labels: dict[str, str], when: float) -> float | None:
        """Return the severity sampled at or just before ``when``, if any."""
        samples = self._by_key.get(_identity_key(labels))
        if not samples:
            return None
        best: float | None = None
        for ts, value in samples:
            if ts <= when:
                best = value
            else:
                break
        return best


def _anomaly_events(
    series_list: list[QuerySeries],
    severity: _SeverityLookup,
) -> list[dict[str, Any]]:
    """Emit an event at each 0->1 transition of the outside-threshold gauge."""
    events: list[dict[str, Any]] = []
    for series in series_list:
        labels = _event_labels(series.labels)
        previous: float | None = None
        for ts, value in _samples(series):
            crossed = value >= 0.5
            if crossed and (previous is None or previous < 0.5):
                events.append(_make_event("anomaly", ts, labels, severity))
            previous = value
    return events


def _change_point_events(
    series_list: list[QuerySeries],
    severity: _SeverityLookup,
) -> list[dict[str, Any]]:
    """Emit an event at each positive increase of the change-point counter."""
    events: list[dict[str, Any]] = []
    for series in series_list:
        labels = _event_labels(series.labels)
        previous: float | None = None
        for ts, value in _samples(series):
            if previous is not None and value > previous:
                events.append(_make_event("change_point", ts, labels, severity))
            previous = value
    return events


def _make_event(
    kind: str,
    when: float,
    labels: dict[str, str],
    severity: _SeverityLookup,
) -> dict[str, Any]:
    """Assemble a single timeline event, attaching severity when available."""
    event: dict[str, Any] = {
        "time": when,
        "time_rfc3339": _format_iso(when),
        "kind": kind,
        "id": labels.get("id", ""),
        "group": labels.get("group", ""),
        "detector": labels.get("detector", ""),
        "detector_instance": labels.get("detector_instance", ""),
    }
    value = severity.at(labels, when)
    if value is not None:
        event["severity"] = value
    return event


def run_timeline(series_by_metric: dict[str, list[QuerySeries]]) -> list[dict[str, Any]]:
    """Compute a time-ordered list of anomaly events from fetched series.

    Args:
        series_by_metric: Fetched series keyed by metric name. Recognised keys
            are ``anomaly_outside_threshold``, ``anomaly_change_point_total``,
            and ``anomaly_severity``; missing keys are treated as empty.

    Returns:
        Events sorted by ascending time. Ties break by kind then ``id`` so the
        ordering is deterministic.
    """
    severity = _SeverityLookup(series_by_metric.get(METRIC_SEVERITY, []))
    events: list[dict[str, Any]] = []
    events.extend(_anomaly_events(series_by_metric.get(METRIC_OUTSIDE, []), severity))
    events.extend(_change_point_events(series_by_metric.get(METRIC_CHANGE_POINT, []), severity))
    events.sort(key=lambda e: (e["time"], e["kind"], e["id"]))
    return events


def format_timeline_text(events: list[dict[str, Any]]) -> str:
    """Render timeline events as a human-readable table."""
    if not events:
        return "No anomaly events in the requested window."
    lines = [f"{'time':<24}  {'kind':<12}  {'severity':>8}  labels"]
    for event in events:
        labels = ", ".join(
            f"{key}={event[key]}"
            for key in ("id", "group", "detector", "detector_instance")
            if event.get(key)
        )
        severity = event.get("severity")
        severity_text = f"{severity:>8.2f}" if isinstance(severity, (int, float)) else f"{'-':>8}"
        ts = event["time_rfc3339"]
        lines.append(f"{ts:<24}  {event['kind']:<12}  {severity_text}  {labels}")
    return "\n".join(lines)
