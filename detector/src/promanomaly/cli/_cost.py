"""``validate --estimate-cost`` — static config cost projection.

Unlike ``--probe`` (which executes every query against the live
datasource to confirm it returns data), ``--estimate-cost`` is a purely
static projection: it never touches the TSDB, so it runs in CI on a
config PR before the change ships. It answers three questions an
operator reviewing a config change wants answered at review time:

* **Cardinality** — projected total output series per group and
  globally, against ``safety.max_total_series``. Worst-case at
  ``safety.max_series_per_query`` because, without a probe, the cap is
  the only bound we can assert.
* **Query load** — projected TSDB queries per refresh interval: the
  per-refresh short-window range queries, the discovery probe queries,
  and the stratified-baseline refreshes (the long multi-week fetches,
  amortised per day).
* **Compute** — a coarse CPU / memory estimate from the documented
  detector resource model (``O(n log n)`` per series on the rolling
  window; memory dominated by one float array per in-flight window).
  Anchored to the guide's "~10k series at a 1-minute refresh on one
  1-CPU replica" reference point.

``--strict`` turns a projected-series overshoot of ``max_total_series``
into a non-zero exit so the check can gate a CI pipeline — the
cardinality complement to ``--probe --strict`` (which gates on whether
the queries actually return data). Flag-name parity with promforecast's
``validate --estimate-cost``.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any

import click

from ..config import Config, parse_duration
from ..detectors import UnknownDetectorError
from ..detectors.registry import get as get_detector
from ..plan import merge_params
from ..stratified import baseline_refresh_seconds_for, query_window_seconds_for

# Documented anchor from the project guide: a single 1-CPU replica
# handles ~10k series at a 1-minute (60s) refresh with the default
# detectors comfortably. The CPU projection scales linearly off this
# point — it is an order-of-magnitude sizing aid, not a benchmark.
_ANCHOR_SERIES = 10_000.0
_ANCHOR_REFRESH_SECONDS = 60.0
# One float array per in-flight rolling window dominates memory; 8 bytes
# per sample plus a small constant per series for the surrounding
# bookkeeping (timestamps, labels, the score frame).
_BYTES_PER_SAMPLE = 8.0
_BYTES_PER_SERIES_OVERHEAD = 512.0


@dataclass
class QueryCostEstimate:
    """Projected cost for one configured query (worst case at the cap)."""

    group: str
    query: str
    is_discovery: bool
    projected_series: int
    detector_count: int
    # Largest per-detector rolling window on this query, in samples
    # (window_seconds / step_seconds) — drives the memory projection.
    window_points: int
    short_queries_per_refresh: int
    discovery_probe_queries_per_refresh: int
    stratified_baseline_queries_per_day: float
    memory_bytes: float
    cpu_cores: float


@dataclass
class GroupCostEstimate:
    """Aggregated projection for one group."""

    group: str
    projected_series: int = 0
    short_queries_per_refresh: int = 0
    discovery_probe_queries_per_refresh: int = 0
    stratified_baseline_queries_per_day: float = 0.0
    recurrence_queries_per_day: float = 0.0
    memory_bytes: float = 0.0
    cpu_cores: float = 0.0
    queries: list[QueryCostEstimate] = field(default_factory=list)


def estimate_query_cost(
    *,
    cfg: Config,
    group_name: str,
    query: Any,
) -> QueryCostEstimate:
    """Project one query's worst-case cost without touching the TSDB.

    Series are projected at ``max_series_per_query`` (the cardinality
    control's per-query ceiling) because static analysis cannot know the
    real fan-out. Discovery queries additionally count one probe per
    declared variable per refresh; the rendered-variant range queries
    are folded into the per-query short-query count as a single ``≥1``
    because the expansion size is only knowable via ``--probe``.
    """
    series_cap = cfg.safety.max_series_per_query
    step_seconds = max(cfg.defaults.step_seconds, 1e-9)
    default_window = cfg.defaults.window_seconds

    detector_count = len(query.detectors)
    # Each input series is scored by every configured detector, so the
    # emitted ``anomaly_score`` family alone is series x detectors. The
    # cardinality cap, however, is enforced on *input* series, so the
    # projected-series figure that matters against ``max_total_series``
    # is the input series cap — detectors multiply the metric *families*,
    # not the series the cap counts.
    projected_series = series_cap

    # The memory / CPU model keys off the rolling *scoring* window (the
    # detector's ``window`` param), not the stratified multi-week
    # lookback: that lookback is fetched once per baseline-refresh and
    # cached, so its cost is a TSDB query cost (counted in
    # ``stratified_baseline_queries_per_day``) rather than a per-refresh
    # dense float array held in memory every tick.
    longest_window_seconds = default_window
    stratified_per_day = 0.0
    for entry in query.detectors:
        try:
            cls = get_detector(entry.name)
        except UnknownDetectorError:
            continue
        merged = merge_params(cls, entry.params, cfg.defaults)
        scoring_window = _scoring_window_seconds(merged, default_window)
        longest_window_seconds = max(longest_window_seconds, scoring_window)
        if query_window_seconds_for(cls, merged) is not None:
            refresh = baseline_refresh_seconds_for(cls, merged)
            if refresh and refresh > 0:
                stratified_per_day += 86400.0 / refresh

    window_points = int(longest_window_seconds / step_seconds)

    is_discovery = bool(getattr(query, "discover", None))
    discovery_probes = len(query.discover) if is_discovery else 0
    # Non-discovery queries issue exactly one short range query per
    # refresh. Discovery queries issue at least one rendered-variant
    # query per refresh (the true count needs --probe).
    short_queries = 1

    # Memory: one float array per in-flight rolling window per series.
    memory_bytes = projected_series * (
        window_points * _BYTES_PER_SAMPLE + _BYTES_PER_SERIES_OVERHEAD
    )
    # CPU: O(n log n) per series per detector, anchored to the documented
    # 10k-series / 60s-refresh / 1-CPU reference. The window-size factor
    # scales the anchor (which assumes the 1h default window) by how much
    # more (or less) work this query's longest window implies.
    anchor_points = max(default_window / step_seconds, 1.0)
    work_factor = _nlogn(window_points) / _nlogn(int(anchor_points))
    refresh_seconds = max(cfg.server.refresh_interval_seconds, 1e-9)
    cpu_cores = (
        (projected_series * detector_count / _ANCHOR_SERIES)
        * (_ANCHOR_REFRESH_SECONDS / refresh_seconds)
        * work_factor
    )

    return QueryCostEstimate(
        group=group_name,
        query=query.id,
        is_discovery=is_discovery,
        projected_series=projected_series,
        detector_count=detector_count,
        window_points=window_points,
        short_queries_per_refresh=short_queries,
        discovery_probe_queries_per_refresh=discovery_probes,
        stratified_baseline_queries_per_day=round(stratified_per_day, 2),
        memory_bytes=memory_bytes,
        cpu_cores=cpu_cores,
    )


def estimate_cost(cfg: Config) -> list[GroupCostEstimate]:
    """Project per-group cost across the whole config."""
    groups: list[GroupCostEstimate] = []
    for group in cfg.groups:
        gest = GroupCostEstimate(group=group.name)
        for query in group.queries:
            qest = estimate_query_cost(cfg=cfg, group_name=group.name, query=query)
            gest.queries.append(qest)
            gest.projected_series += qest.projected_series
            gest.short_queries_per_refresh += qest.short_queries_per_refresh
            gest.discovery_probe_queries_per_refresh += qest.discovery_probe_queries_per_refresh
            gest.stratified_baseline_queries_per_day += qest.stratified_baseline_queries_per_day
            gest.memory_bytes += qest.memory_bytes
            gest.cpu_cores += qest.cpu_cores
        # Recurrence analysis: one TSDB range query per refresh_interval
        # (not per group refresh), amortised to queries-per-day.
        if group.recurrence.enabled:
            recurrence_interval = group.recurrence.refresh_interval_seconds
            if recurrence_interval > 0:
                gest.recurrence_queries_per_day = round(86400.0 / recurrence_interval, 2)
        groups.append(gest)
    return groups


def run_estimate_cost(cfg: Config, *, strict: bool) -> int:
    """Print the JSON-lines cost projection; return a CLI exit code.

    One ``{"group": ..., "status": "estimate", ...}`` line per group,
    then a ``{"summary": {...}}`` line. ``strict`` makes a projected
    total over ``safety.max_total_series`` exit non-zero.
    """
    groups = estimate_cost(cfg)
    projected_total = 0
    total_memory = 0.0
    total_cpu = 0.0
    short_total = 0
    probe_total = 0
    stratified_total = 0.0
    recurrence_total = 0.0
    has_discovery = False
    for gest in groups:
        projected_total += gest.projected_series
        total_memory += gest.memory_bytes
        total_cpu += gest.cpu_cores
        short_total += gest.short_queries_per_refresh
        probe_total += gest.discovery_probe_queries_per_refresh
        stratified_total += gest.stratified_baseline_queries_per_day
        recurrence_total += gest.recurrence_queries_per_day
        group_has_discovery = any(q.is_discovery for q in gest.queries)
        has_discovery = has_discovery or group_has_discovery
        entry: dict[str, Any] = {
            "group": gest.group,
            "status": "estimate",
            "projected_series": gest.projected_series,
            "short_queries_per_refresh": gest.short_queries_per_refresh,
            "discovery_probe_queries_per_refresh": (gest.discovery_probe_queries_per_refresh),
            "stratified_baseline_queries_per_day": round(
                gest.stratified_baseline_queries_per_day, 2
            ),
            "recurrence_queries_per_day": gest.recurrence_queries_per_day,
            "estimated_memory_bytes": int(gest.memory_bytes),
            "estimated_cpu_cores": round(gest.cpu_cores, 3),
        }
        if group_has_discovery:
            # A discover: query fans out to N rendered variants at run
            # time, each up to max_series_per_query. Static analysis can't
            # know N, so projected_series counts one variant's worth — a
            # lower bound. Flag it so an operator never reads a discovery
            # group's "under budget" as a guarantee; run --probe for the
            # real fan-out against the live TSDB.
            entry["projected_series_is_lower_bound"] = True
        click.echo(json.dumps(entry, sort_keys=True))
    over_budget = projected_total > cfg.safety.max_total_series
    summary: dict[str, Any] = {
        "projected_total_series": projected_total,
        "max_total_series": cfg.safety.max_total_series,
        "over_budget": over_budget,
        "short_queries_per_refresh": short_total,
        "discovery_probe_queries_per_refresh": probe_total,
        "stratified_baseline_queries_per_day": round(stratified_total, 2),
        "recurrence_queries_per_day": round(recurrence_total, 2),
        "estimated_memory_bytes": int(total_memory),
        "estimated_cpu_cores": round(total_cpu, 3),
        "refresh_interval_seconds": cfg.server.refresh_interval_seconds,
    }
    if has_discovery:
        # The total excludes discovery fan-out (see per-group flag), so it
        # is a lower bound — don't let a green "under budget" mislead.
        summary["projected_series_is_lower_bound"] = True
        summary["discovery_note"] = (
            "projected_total_series excludes discover: fan-out; "
            "run --probe for the real expansion against max_total_series"
        )
    click.echo(json.dumps({"summary": summary}, sort_keys=True))
    if over_budget:
        # Always surface the overshoot; only fail the process under
        # --strict so the advisory mode stays usable interactively.
        click.echo(
            json.dumps(
                {
                    "status": "cardinality_exceeded",
                    "projected_total_series": projected_total,
                    "max_total_series": cfg.safety.max_total_series,
                },
                sort_keys=True,
            ),
            err=True,
        )
        if strict:
            return 1
    return 0


def _scoring_window_seconds(merged: dict[str, Any], default_window: float) -> float:
    """Resolve a detector's rolling scoring window from its merged params.

    ``merge_params`` always seeds ``window`` from the group default, so
    the key is present; a malformed value falls back to the default
    rather than crashing the static estimate.
    """
    raw = merged.get("window")
    if raw is None:
        return default_window
    try:
        return parse_duration(raw)
    except (ValueError, TypeError):
        return default_window


def _nlogn(points: int) -> float:
    """``n log2 n`` with a floor so tiny / zero windows don't blow up."""
    n = max(points, 1)
    return n * math.log2(n + 1)


__all__ = [
    "GroupCostEstimate",
    "QueryCostEstimate",
    "estimate_cost",
    "estimate_query_cost",
    "run_estimate_cost",
]
