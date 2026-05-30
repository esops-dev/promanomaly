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

from .detectors.cohort import cohort_key_for

if TYPE_CHECKING:  # pragma: no cover - import only for type hints
    from .state import SnapshotStore

# Canonical labels stamped by the detector; everything else on a sample
# is a source label echoed from the query.
_CANONICAL = frozenset({"id", "group", "detector", "detector_instance"})

# Labels that are not part of a series' fleet identity: the detector axis
# (so the same source series counted once regardless of how many detectors
# scored it) and the cohort meta-label. Mirrors the density rollup's rule.
_IDENTITY_DROP = frozenset({"detector", "detector_instance", "cohort_label"})


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


@dataclass(frozen=True)
class BlastRadiusRow:
    """Co-firing rollup for one group or one cohort in the current run.

    A point-in-time read off the current snapshot — not a stateful
    correlation engine and not tied to any event identity. ``firing`` is
    how many distinct members of the scope are over their threshold right
    now, ``total`` is how many members the scope has at all, ``fraction``
    is their ratio, and ``max_co_firing_seconds`` is the longest any of
    the co-firing members has been continuously breached.
    """

    scope: str
    group: str
    firing: int
    total: int
    fraction: float
    max_co_firing_seconds: float
    cohort_label: str | None = None
    cohort_key: dict[str, str] | None = None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "scope": self.scope,
            "group": self.group,
            "firing": self.firing,
            "total": self.total,
            "fraction": self.fraction,
            "max_co_firing_seconds": self.max_co_firing_seconds,
        }
        if self.cohort_label is not None:
            out["cohort_label"] = self.cohort_label
        if self.cohort_key is not None:
            out["cohort_key"] = dict(self.cohort_key)
        return out


def _series_identity(labels: dict[str, str]) -> tuple[tuple[str, str], ...]:
    """Fleet identity of a series — drops detector and cohort meta-labels.

    The same source series scored by several detectors collapses to one
    identity, matching the density rollup so the two never disagree on how
    many members a scope has.
    """
    items = {k: v for k, v in labels.items() if k not in _IDENTITY_DROP}
    return tuple(sorted(items.items()))


def collect_blast_radius(store: SnapshotStore) -> list[BlastRadiusRow]:
    """Compute per-group and per-cohort co-firing rollups for the current run.

    Args:
        store: the live snapshot store. Only the most recent snapshot per
            group is read; no TSDB queries are issued.

    For each group, and for each cohort within a group whose samples carry
    a ``cohort_label`` axis, the rollup reports how many members are firing
    now, the total member count, the firing fraction, and the longest
    continuous breach across the co-firing members. Group-level counts are
    taken from the already-emitted ``anomaly_active_series`` /
    ``anomaly_density`` rollup samples when present (recovering the total as
    ``round(active / fraction)``), and recomputed from
    ``anomaly_outside_threshold`` samples otherwise. Series still warming up
    are excluded from both counts, consistent with the density rollup.

    Returns:
        Rows sorted by fraction descending, then firing count descending,
        then ``(group, cohort_key)`` for a stable order.
    """
    # Per group: identity sets for total / firing, the longest breach among
    # firing members, and the group-level rollup samples if the runner
    # already emitted them.
    group_total: dict[str, set[tuple[tuple[str, str], ...]]] = {}
    group_firing: dict[str, set[tuple[tuple[str, str], ...]]] = {}
    group_warming: dict[str, set[tuple[tuple[str, str], ...]]] = {}
    group_max_duration: dict[str, float] = {}
    group_active: dict[str, float] = {}
    group_density: dict[str, float] = {}

    # Per cohort: keyed by (group, cohort_label, cohort_key) so two axes in
    # the same group stay separate.
    CohortId = tuple[str, str, tuple[tuple[str, str], ...]]
    cohort_total: dict[CohortId, set[tuple[tuple[str, str], ...]]] = {}
    cohort_firing: dict[CohortId, set[tuple[tuple[str, str], ...]]] = {}
    cohort_warming: dict[CohortId, set[tuple[tuple[str, str], ...]]] = {}
    cohort_max_duration: dict[CohortId, float] = {}

    # Durations are joined on the full per-detector key so the longest
    # breach is read from the same series that is firing.
    durations: dict[tuple[tuple[str, str], ...], float] = {}

    for snap in store.all_snapshots():
        for sample in snap.samples:
            labels = dict(sample.labels)
            grp = labels.get("group", "")
            metric = sample.metric
            if metric == "anomaly_duration_seconds":
                durations[_detector_key(labels)] = sample.value
            elif metric == "anomaly_active_series" and set(labels) == {"group"}:
                group_active[grp] = sample.value
            elif metric == "anomaly_density" and set(labels) == {"group"}:
                group_density[grp] = sample.value

    for snap in store.all_snapshots():
        for sample in snap.samples:
            labels = dict(sample.labels)
            if sample.metric == "anomaly_warming_up" and sample.value >= 1.0:
                grp = labels.get("group", "")
                ident = _series_identity(labels)
                group_warming.setdefault(grp, set()).add(ident)
                axis = labels.get("cohort_label")
                if axis is not None:
                    key = cohort_key_for(labels, axis)
                    cohort_warming.setdefault((grp, axis, key), set()).add(ident)

    for snap in store.all_snapshots():
        for sample in snap.samples:
            if sample.metric != "anomaly_outside_threshold":
                continue
            labels = dict(sample.labels)
            grp = labels.get("group", "")
            ident = _series_identity(labels)
            group_total.setdefault(grp, set()).add(ident)
            firing_now = sample.value >= 1.0
            if firing_now:
                group_firing.setdefault(grp, set()).add(ident)
                breach = durations.get(_detector_key(labels), 0.0)
                group_max_duration[grp] = max(group_max_duration.get(grp, 0.0), breach)
            axis = labels.get("cohort_label")
            if axis is not None:
                key = cohort_key_for(labels, axis)
                cid = (grp, axis, key)
                cohort_total.setdefault(cid, set()).add(ident)
                if firing_now:
                    cohort_firing.setdefault(cid, set()).add(ident)
                    breach = durations.get(_detector_key(labels), 0.0)
                    cohort_max_duration[cid] = max(cohort_max_duration.get(cid, 0.0), breach)

    rows: list[BlastRadiusRow] = []

    groups = set(group_total) | set(group_active) | set(group_density)
    for grp in groups:
        warming = group_warming.get(grp, set())
        firing = len(group_firing.get(grp, set()) - warming)
        total = len((group_total.get(grp, set())) - warming)
        # Prefer the runner's already-emitted rollup when it is present —
        # that is the "built on the density rollup" intent.
        if grp in group_active:
            firing = round(group_active[grp])
        density = group_density.get(grp)
        if density is not None and density > 0:
            total = round(group_active.get(grp, float(firing)) / density)
        fraction = firing / total if total else 0.0
        rows.append(
            BlastRadiusRow(
                scope="group",
                group=grp,
                firing=firing,
                total=total,
                fraction=fraction,
                max_co_firing_seconds=group_max_duration.get(grp, 0.0),
            )
        )

    for cid in cohort_total:
        grp, axis, key = cid
        warming = cohort_warming.get(cid, set())
        firing = len(cohort_firing.get(cid, set()) - warming)
        total = len(cohort_total[cid] - warming)
        fraction = firing / total if total else 0.0
        rows.append(
            BlastRadiusRow(
                scope="cohort",
                group=grp,
                cohort_label=axis,
                cohort_key=dict(key),
                firing=firing,
                total=total,
                fraction=fraction,
                max_co_firing_seconds=cohort_max_duration.get(cid, 0.0),
            )
        )

    rows.sort(
        key=lambda r: (
            -r.fraction,
            -r.firing,
            r.group,
            tuple(sorted((r.cohort_key or {}).items())),
        )
    )
    return rows


__all__ = ["BlastRadiusRow", "FiringSeries", "collect_blast_radius", "collect_firing"]
