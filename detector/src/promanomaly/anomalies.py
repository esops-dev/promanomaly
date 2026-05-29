"""Live triage — assemble the currently-firing series, ranked by severity.

Powers the ``/debug/anomalies`` endpoint and the ``promanomaly top`` CLI.
The on-call question is "what is anomalous right now, ranked?" — answered
without opening Grafana or hand-writing PromQL, the same way ``top``
answers "what is eating this host right now".

The data is read straight from the in-memory snapshot store — the same
samples ``/metrics`` is serving — so there are no extra TSDB queries and
the answer is exactly consistent with what a scrape would show. A firing
is one ``(id, group, detector, source-labels)`` series whose
``anomaly_outside_threshold == 1``; its score / severity / duration /
type are joined from the sibling samples sharing the same label set.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - import only for type hints
    from .state import SnapshotStore

# Canonical labels stamped by the detector; everything else on a sample
# is a source label echoed from the query.
_CANONICAL = frozenset({"id", "group", "detector", "detector_instance"})


@dataclass(frozen=True)
class FiringSeries:
    """One currently-firing detector series, with its triage fields."""

    id: str
    group: str
    detector: str
    detector_instance: str | None
    score: float
    severity: float
    duration_seconds: float
    type: str | None
    labels: dict[str, str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "group": self.group,
            "detector": self.detector,
            "detector_instance": self.detector_instance,
            "score": self.score,
            "severity": self.severity,
            "duration_seconds": self.duration_seconds,
            "type": self.type,
            "labels": dict(self.labels),
        }


def _detector_key(labels: dict[str, str]) -> tuple[tuple[str, str], ...]:
    """Full per-detector identity (includes detector + detector_instance)."""
    return tuple(sorted(labels.items()))


def collect_firing(
    store: SnapshotStore,
    *,
    group: str | None = None,
    min_severity: float = 0.0,
    limit: int | None = None,
) -> list[FiringSeries]:
    """Return currently-firing series across all groups, ranked by severity.

    Args:
        store: the live snapshot store.
        group: restrict to one group when set.
        min_severity: drop firings below this severity (0-1).
        limit: keep at most this many after ranking.

    Ranking is by severity descending, then score descending, then
    ``(group, id, detector)`` for a stable order so two scrapes with
    equal severities don't shuffle the table.
    """
    # Index sibling samples by full per-detector label key so we can join
    # score / severity / duration / type onto each firing.
    scores: dict[tuple[tuple[str, str], ...], float] = {}
    severities: dict[tuple[tuple[str, str], ...], float] = {}
    durations: dict[tuple[tuple[str, str], ...], float] = {}
    types: dict[tuple[tuple[str, str], ...], str] = {}
    firing_keys: list[tuple[tuple[str, str], ...]] = []
    firing_labels: dict[tuple[tuple[str, str], ...], dict[str, str]] = {}

    for snap in store.all_snapshots():
        for sample in snap.samples:
            labels = dict(sample.labels)
            if group is not None and labels.get("group") != group:
                continue
            metric = sample.metric
            if metric == "anomaly_outside_threshold":
                if sample.value >= 1.0:
                    key = _detector_key(labels)
                    firing_keys.append(key)
                    firing_labels[key] = labels
            elif metric == "anomaly_score":
                scores[_detector_key(labels)] = sample.value
            elif metric == "anomaly_severity":
                severities[_detector_key(labels)] = sample.value
            elif metric == "anomaly_duration_seconds":
                durations[_detector_key(labels)] = sample.value
            elif metric == "anomaly_type_score":
                # The type metric carries an extra ``type`` label; strip it
                # so the key joins onto the per-detector series.
                anomaly_kind = labels.get("type")
                joinable = {k: v for k, v in labels.items() if k != "type"}
                if anomaly_kind is not None:
                    types[_detector_key(joinable)] = anomaly_kind

    results: list[FiringSeries] = []
    for key in firing_keys:
        labels = firing_labels[key]
        severity = severities.get(key, 0.0)
        if severity < min_severity:
            continue
        source_labels = {k: v for k, v in labels.items() if k not in _CANONICAL}
        results.append(
            FiringSeries(
                id=labels.get("id", ""),
                group=labels.get("group", ""),
                detector=labels.get("detector", ""),
                detector_instance=labels.get("detector_instance"),
                score=scores.get(key, 0.0),
                severity=severity,
                duration_seconds=durations.get(key, 0.0),
                type=types.get(key),
                labels=source_labels,
            )
        )

    results.sort(key=lambda f: (-f.severity, -f.score, f.group, f.id, f.detector))
    if limit is not None:
        results = results[:limit]
    return results


__all__ = ["FiringSeries", "collect_firing"]
