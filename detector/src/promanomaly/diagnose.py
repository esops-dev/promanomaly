"""Config health and tuning diagnostics (``promanomaly diagnose``).

Tells an operator which of their detectors are mis-tuned, from evidence
rather than guesswork:

* **never fires** — dead config: a detector that hasn't crossed threshold
  over the whole lookback (wrong detector, threshold too high, or a query
  that doesn't carry the signal it's meant to).
* **fires often** — threshold too low or the wrong detector: a detector
  firing more than a configurable fraction of the time is alert noise.
* **chronically warming** — series stuck below ``min_points`` for the
  whole window (a too-sparse query or a ``min_points`` set too high).
* **empty query** — a query that returned no series over the window.
* **silent detector** — (with ``--config``) a configured detector that
  emitted no score at all, which includes cohort detectors silently
  dropping cohorts below ``min_cohort_size``.

Two data sources, sharing one report shape:

* **TSDB lookback** (``--datasource-url``) — instant queries that
  aggregate the detector's own metrics over ``--window``. This is the
  mode that answers "firing rate over a lookback"; ``never_fires`` /
  ``fires_often`` are only meaningful here.
* **live snapshot** (``--target``) — one scrape of the detector's
  ``/metrics``, for a quick current-state read (firing now, warming now,
  empty now). It cannot compute a rate, so it never reports
  ``never_fires``.

Pure analysis, no persisted state — parity with promforecast's
``diagnose``. Pairs with ``backtest`` and ``calibrate-buckets`` to close
the tuning loop.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from .config import Config

# Default bands. A detector firing more than half the lookback is almost
# certainly mis-tuned; one that never fires is dead config. Both are
# advisory and overridable on the CLI.
DEFAULT_FIRE_HIGH = 0.5
# A firing rate at or below this counts as "never fires" — a tiny epsilon
# rather than exactly 0 so a single stray sample in a long window doesn't
# rescue a detector that is effectively dead.
DEFAULT_NEVER_EPS = 0.0

InstantRows = list[tuple[dict[str, str], float]]


@dataclass(frozen=True)
class DiagnoseFinding:
    """One tuning issue found for a group / query / detector."""

    category: str
    group: str
    query: str
    detector: str
    detail: str
    value: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "group": self.group,
            "query": self.query,
            "detector": self.detector,
            "detail": self.detail,
            "value": self.value,
        }


@dataclass(frozen=True)
class DiagnoseReport:
    """The full diagnose result."""

    mode: str
    window: str | None
    findings: list[DiagnoseFinding]

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "window": self.window,
            "findings": [f.to_dict() for f in self.findings],
        }


def _detector_label(labels: dict[str, str]) -> str:
    detector = labels.get("detector", "")
    instance = labels.get("detector_instance")
    return f"{detector}[{instance}]" if instance else detector


def evaluate_firing_rates(
    rows: InstantRows,
    *,
    fire_high: float = DEFAULT_FIRE_HIGH,
    never_eps: float = DEFAULT_NEVER_EPS,
) -> list[DiagnoseFinding]:
    """Flag per-detector firing rates outside the never / often bands.

    ``rows`` come from ``avg by (id, group, detector, detector_instance)
    (avg_over_time(anomaly_outside_threshold[W]))`` — one row per detector
    series, value in ``[0, 1]``.
    """
    findings: list[DiagnoseFinding] = []
    for labels, rate in rows:
        group = labels.get("group", "")
        query = labels.get("id", "")
        det = _detector_label(labels)
        if rate <= never_eps:
            findings.append(
                DiagnoseFinding(
                    category="never_fires",
                    group=group,
                    query=query,
                    detector=det,
                    detail=(
                        "detector never crossed threshold over the lookback — "
                        "dead config: wrong detector, threshold too high, or the "
                        "query doesn't carry the signal"
                    ),
                    value=rate,
                )
            )
        elif rate >= fire_high:
            findings.append(
                DiagnoseFinding(
                    category="fires_often",
                    group=group,
                    query=query,
                    detector=det,
                    detail=(
                        f"detector fired {rate:.0%} of the lookback — threshold "
                        "likely too low or the wrong detector for this signal"
                    ),
                    value=rate,
                )
            )
    return findings


def evaluate_warming(rows: InstantRows) -> list[DiagnoseFinding]:
    """Flag series stuck warming up for the whole window.

    ``rows`` come from ``count by (group, id)
    (min_over_time(anomaly_warming_up[W]) >= 1)`` — value is the number of
    series still warming across the entire lookback.
    """
    findings: list[DiagnoseFinding] = []
    for labels, count in rows:
        if count <= 0:
            continue
        findings.append(
            DiagnoseFinding(
                category="chronically_warming",
                group=labels.get("group", ""),
                query=labels.get("id", ""),
                detector="",
                detail=(
                    f"{int(count)} series have been warming up the whole lookback — "
                    "the query is too sparse or min_points is set too high"
                ),
                value=float(count),
            )
        )
    return findings


def evaluate_empty(rows: InstantRows) -> list[DiagnoseFinding]:
    """Flag groups whose series count was zero over the window.

    ``rows`` come from ``max_over_time(anomaly_series_count[W])`` — one row
    per group, value the peak series count.
    """
    findings: list[DiagnoseFinding] = []
    for labels, peak in rows:
        if peak > 0:
            continue
        findings.append(
            DiagnoseFinding(
                category="empty_query",
                group=labels.get("group", ""),
                query="",
                detector="",
                detail="group produced no series over the lookback — check the PromQL",
                value=float(peak),
            )
        )
    return findings


def evaluate_silent_detectors(
    config: Config, emitting: set[tuple[str, str]]
) -> list[DiagnoseFinding]:
    """Flag configured detectors that emitted no score over the window.

    ``emitting`` is the set of ``(group, detector)`` pairs that produced
    at least one ``anomaly_score`` series. A configured detector missing
    from it is silent — dead config, or a cohort detector dropping every
    cohort below ``min_cohort_size``.
    """
    findings: list[DiagnoseFinding] = []
    seen: set[tuple[str, str, str]] = set()
    for group in config.groups:
        for query in group.queries:
            for det in query.detectors:
                key = (group.name, det.name)
                dedup = (group.name, query.id, det.name)
                if dedup in seen:
                    continue
                seen.add(dedup)
                if key in emitting:
                    continue
                cohort_note = (
                    " (cohort detectors emit nothing when every cohort is below min_cohort_size)"
                    if det.name == "Cohort"
                    else ""
                )
                findings.append(
                    DiagnoseFinding(
                        category="silent_detector",
                        group=group.name,
                        query=query.id,
                        detector=det.name,
                        detail=(
                            "configured detector emitted no anomaly_score over the "
                            f"lookback{cohort_note}"
                        ),
                        value=0.0,
                    )
                )
    return findings


def diagnose_from_metrics_text(
    text: str, *, fire_high: float = DEFAULT_FIRE_HIGH
) -> DiagnoseReport:
    """Current-state diagnose from one ``/metrics`` scrape (``--target``).

    Reports empty queries, currently-warming series, and detectors firing
    on a large fraction of their series *right now*. A single snapshot
    cannot establish a rate over time, so ``never_fires`` is deliberately
    not reported here — use the TSDB lookback mode for that.
    """
    from prometheus_client.parser import text_string_to_metric_families

    series_count_by_group: dict[str, float] = {}
    warming_by_query: dict[tuple[str, str], int] = defaultdict(int)
    firing_by_detector: dict[tuple[str, str, str], list[int]] = defaultdict(lambda: [0, 0])

    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            name = sample.name
            labels = sample.labels
            value = sample.value
            group = labels.get("group", "")
            if name == "anomaly_series_count":
                series_count_by_group[group] = value
            elif name == "anomaly_warming_up" and value >= 1:
                warming_by_query[(group, labels.get("id", ""))] += 1
            elif name == "anomaly_outside_threshold":
                key = (group, labels.get("id", ""), _detector_label(labels))
                bucket = firing_by_detector[key]
                bucket[1] += 1
                if value >= 1:
                    bucket[0] += 1

    findings: list[DiagnoseFinding] = []
    for group, count in sorted(series_count_by_group.items()):
        if count <= 0:
            findings.append(
                DiagnoseFinding(
                    category="empty_query",
                    group=group,
                    query="",
                    detector="",
                    detail="group currently has zero series — check the PromQL",
                    value=count,
                )
            )
    for (group, query_id), n in sorted(warming_by_query.items()):
        findings.append(
            DiagnoseFinding(
                category="warming",
                group=group,
                query=query_id,
                detector="",
                detail=f"{n} series are warming up right now",
                value=float(n),
            )
        )
    for (group, query_id, det), (active, total) in sorted(firing_by_detector.items()):
        if total == 0:
            continue
        frac = active / total
        if frac >= fire_high:
            findings.append(
                DiagnoseFinding(
                    category="fires_often",
                    group=group,
                    query=query_id,
                    detector=det,
                    detail=(
                        f"{active}/{total} series firing right now ({frac:.0%}) — "
                        "threshold likely too low or the wrong detector"
                    ),
                    value=frac,
                )
            )
    return DiagnoseReport(mode="snapshot", window=None, findings=findings)


def render_report(report: DiagnoseReport) -> str:
    """Render a diagnose report as human-readable text."""
    lines: list[str] = []
    scope = f"mode={report.mode}"
    if report.window:
        scope += f" window={report.window}"
    lines.append(f"promanomaly diagnose ({scope})")
    if not report.findings:
        lines.append("  no tuning issues found")
        return "\n".join(lines) + "\n"
    by_category: dict[str, list[DiagnoseFinding]] = defaultdict(list)
    for finding in report.findings:
        by_category[finding.category].append(finding)
    for category in sorted(by_category):
        lines.append(f"\n[{category}] ({len(by_category[category])})")
        for f in by_category[category]:
            target = "/".join(p for p in (f.group, f.query, f.detector) if p)
            lines.append(f"  - {target}: {f.detail}")
    return "\n".join(lines) + "\n"


__all__ = [
    "DEFAULT_FIRE_HIGH",
    "DEFAULT_NEVER_EPS",
    "DiagnoseFinding",
    "DiagnoseReport",
    "diagnose_from_metrics_text",
    "evaluate_empty",
    "evaluate_firing_rates",
    "evaluate_silent_detectors",
    "evaluate_warming",
    "render_report",
]
