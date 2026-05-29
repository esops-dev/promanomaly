"""Metadata-aware validation lint.

The TSDB already knows each metric's type via ``/api/v1/metadata``. This
module uses that to catch a common config mistake at validate time
rather than in production: a **counter fed to a detector without being
wrapped in ``rate()`` / ``increase()``**. A raw counter monotonically
climbs, so every statistical baseline detector (MAD, Hampel, Z-score,
IQR, the stratified detectors) sees an ever-rising ramp and scores
nonsense; the operator usually meant ``rate(http_requests_total[5m])``.

It is **lint only**. It reports; under ``--strict`` it can fail CI; it
never rewrites the query or auto-applies ``rate()`` — that would violate
promanomaly's explicit, config-driven principle. Absent metadata is
treated as "no opinion" (no finding), so a metric the TSDB doesn't carry
metadata for, or a metadata endpoint that returns nothing, never
produces a false positive. The same findings are surfaced live in
``inspect`` and ``promanomaly top``.

The PromQL analysis is deliberately a lightweight regex pass, not a full
parser: it only needs to answer "does this known counter appear without
a rate-like wrapper?" — and it only ever looks at metric names the TSDB
metadata confirms are counters, so an over-broad match cannot
manufacture a finding for a metric it knows nothing about.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .config import Config, QueryConfig

# Functions that turn a counter into a rate / delta, making a counter
# input legitimate. A counter appearing only inside one of these is
# "rate-wrapped" and never flagged.
_COUNTER_FUNCTIONS: tuple[str, ...] = (
    "rate",
    "irate",
    "increase",
    "delta",
    "idelta",
    "deriv",
    "resets",
)

# Series-identity aggregations that use only the *presence* of a series,
# not its value — ``count(node_cpu_seconds_total)`` counts cores, it does
# not score the raw counter. Treating the counter as safe inside these
# avoids the most common false positive (a counter used purely as a
# denominator's series count) without a full PromQL parser.
_IDENTITY_FUNCTIONS: tuple[str, ...] = (
    "count",
    "count_values",
    "group",
    "absent",
    "absent_over_time",
    "present_over_time",
)

# PromQL identifier (metric or function name). Colons are allowed because
# recording-rule names carry them; our own output never does.
_IDENT_RE = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*")

# A function call wrapping a metric: capture the first identifier inside
# the call. Handles the nested case (``sum(rate(foo_total[5m]))``) because
# the wrapper directly precedes the metric regardless of any outer
# aggregation. Both rate-like and series-identity functions count as a
# legitimate wrapper for a counter.
_WRAP_RE = re.compile(
    r"\b(?:"
    + "|".join((*_COUNTER_FUNCTIONS, *_IDENTITY_FUNCTIONS))
    + r")\s*"
    # Optional aggregation modifier: ``count by (instance) (metric)`` /
    # ``sum without (cpu) (metric)``. Rate-like functions never carry one,
    # so making it optional is harmless for them.
    + r"(?:(?:by|without)\s*\([^)]*\)\s*)?"
    + r"\(\s*([a-zA-Z_:][a-zA-Z0-9_:]*)"
)


@dataclass(frozen=True)
class MetadataFinding:
    """One metadata/detector mismatch for a configured query."""

    group: str
    query_id: str
    metric: str
    metric_type: str
    issue: str
    detectors: list[str]
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "group": self.group,
            "query": self.query_id,
            "metric": self.metric,
            "metric_type": self.metric_type,
            "issue": self.issue,
            "detectors": list(self.detectors),
            "message": self.message,
        }


def normalise_metadata(payload: Any) -> dict[str, str]:
    """Reduce a ``/api/v1/metadata`` ``data`` map to ``{metric: type}``.

    Accepts the already-reduced ``{metric: type}`` dict (what
    :meth:`PromQLSource.metric_metadata` returns) unchanged, or the raw
    ``{metric: [{"type": ...}, ...]}`` shape, so callers can pass either.
    """
    if not isinstance(payload, dict):
        return {}
    out: dict[str, str] = {}
    for metric, value in payload.items():
        if not isinstance(metric, str):
            continue
        if isinstance(value, str):
            out[metric] = value
        elif isinstance(value, list) and value and isinstance(value[0], dict):
            metric_type = value[0].get("type")
            if isinstance(metric_type, str) and metric_type:
                out[metric] = metric_type
    return out


def unrated_counters(promql: str, metadata: dict[str, str]) -> list[str]:
    """Counter metrics in ``promql`` used without a rate-like wrapper.

    Only metrics the metadata confirms are counters are considered, so a
    metric the TSDB has no opinion on is never flagged. A counter that
    appears anywhere inside ``rate()`` / ``increase()`` / … is treated as
    correctly wrapped.
    """
    idents = set(_IDENT_RE.findall(promql))
    counters_used = {m for m in idents if metadata.get(m) == "counter"}
    if not counters_used:
        return []
    wrapped = set(_WRAP_RE.findall(promql))
    return sorted(counters_used - wrapped)


def lint_query(
    group_name: str,
    query: QueryConfig,
    metadata: dict[str, str],
) -> list[MetadataFinding]:
    """Lint one query against the TSDB metric metadata."""
    findings: list[MetadataFinding] = []
    detector_names = [d.name for d in query.detectors]
    for metric in unrated_counters(query.promql, metadata):
        det_repr = ", ".join(detector_names) or "(none)"
        findings.append(
            MetadataFinding(
                group=group_name,
                query_id=query.id,
                metric=metric,
                metric_type="counter",
                issue="counter_not_rated",
                detectors=detector_names,
                message=(
                    f"metric {metric!r} is a counter used without "
                    f"rate()/increase(); detector(s) [{det_repr}] would score "
                    "the raw monotonically-increasing value. Wrap it in "
                    f"rate({metric}[...]) or increase({metric}[...])."
                ),
            )
        )
    return findings


def lint_config_metadata(config: Config, metadata: dict[str, str]) -> list[MetadataFinding]:
    """Lint every query in ``config`` against the metric metadata map."""
    findings: list[MetadataFinding] = []
    for group in config.groups:
        for query in group.queries:
            findings.extend(lint_query(group.name, query, metadata))
    return findings


__all__ = [
    "MetadataFinding",
    "lint_config_metadata",
    "lint_query",
    "normalise_metadata",
    "unrated_counters",
]
