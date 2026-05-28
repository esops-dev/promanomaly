"""Ensemble fusion: collapse per-detector outcomes into composite metrics.

Two methods only — ``max`` (logical OR across detectors) and ``voting``
(at least N agree). Weighted voting, Bayesian fusion, and ML-trained
combiners are deliberately out of scope per the project guide; operators
who need richer combination compose via recording rules.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from .config import EnsembleConfig, GroupConfig, QueryConfig
from .source import QuerySeries
from .state import Sample


@dataclass
class DetectorOutcome:
    """Per-detector score + alerting bit for one series.

    Captured per series so the ensemble fuser can fold the individual
    detector verdicts into a composite without re-running detectors.
    The runner retains these only long enough to compute composites
    and discards them.
    """

    score: float
    outside: bool


def fuse(
    *,
    group: GroupConfig,
    query: QueryConfig,
    series: QuerySeries,
    source_labels: dict[str, str],
    outcomes: Iterable[DetectorOutcome],
    ensemble: EnsembleConfig,
) -> list[Sample]:
    """Fuse per-detector outcomes into composite metrics.

    ``max`` is a logical OR across detectors; ``voting`` fires only when
    ``ensemble.min_detectors`` agree. The composite score is the max
    raw score across detectors in both modes, so the gauge stays useful
    as a "how loud is the loudest detector" indicator independent of
    the firing rule.

    ``source_labels`` must already be passed through the exporter's
    output-label filter — fusion intentionally does not know about
    label-allow/drop policy.
    """
    # ``id`` and ``group`` are the only first-class labels on composite
    # metrics — we drop ``detector`` because the composite spans
    # multiple detectors, and additional source-side labels are
    # filtered upstream to keep fan-in cardinality bounded.
    outcomes_list = list(outcomes)

    result: dict[str, str] = dict(source_labels)
    result["id"] = query.id
    result["group"] = group.name
    labels = tuple(sorted(result.items()))

    composite_score = max((o.score for o in outcomes_list), default=0.0)
    if ensemble.method == "max":
        fires = any(o.outside for o in outcomes_list)
    elif ensemble.method == "voting":
        agree = sum(1 for o in outcomes_list if o.outside)
        fires = agree >= ensemble.min_detectors
    else:
        # Defensive: the schema restricts ``method`` to {max, voting},
        # so this branch only trips on a future enum addition. Treat
        # as "do not emit" rather than crashing the whole group.
        return []
    return [
        Sample(metric="anomaly_composite_score", labels=labels, value=composite_score),
        Sample(
            metric="anomaly_composite_outside_threshold",
            labels=labels,
            value=1.0 if fires else 0.0,
        ),
    ]


__all__ = ["DetectorOutcome", "fuse"]
