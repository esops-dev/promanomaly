"""Detector warm-up status.

Reports per-(group, query) "what is still loading?" status for a fresh
install or a just-restarted pod. It answers the questions an operator
asks during the first cycle after ``helm install`` when the dashboard is
empty:

* How many rolling-window samples does this query already have?
* How many does it need before the detector will score (``min_points``)?
* What is currently blocking it (no data yet, not enough points, a
  stratified baseline still filling its multi-week lookback)?
* Roughly how long until the first usable score?

The status is computed on demand — there is no background poll. Each
call issues one cheap range query per configured query (a small
``range_query`` over the rolling window, returning at most
``window / step`` samples). Probes fan out concurrently across queries
and across groups, so total wall time is bounded by the slowest probe
rather than the sum.

The endpoint is opt-in via ``server.expose_warmup_endpoint`` because
even the one-probe-per-query cost is wasted on the steady-state path:
the operator workflow is "hit it a few times during the first refresh,
then never again". For the same reason there is no caching layer — a
stale cached answer is more confusing than a fresh probe.

Mirrors promforecast's warmup surface so the cross-tool operator
experience is identical (``promanomaly warmup`` ↔ ``promforecast
warmup``, same JSON shape and table columns).
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import structlog

from .config import Config, DefaultsConfig, GroupConfig, QueryConfig
from .detectors import UnknownDetectorError
from .detectors.registry import get as get_detector
from .plan import merge_params
from .source import SourceQueryError
from .stratified import query_window_seconds_for

if TYPE_CHECKING:
    from .source import PromQLSource

logger = structlog.get_logger(__name__)


# Bounded set of ``blocked_by`` reasons. ``lookback`` = the TSDB returned
# no samples at all; ``min_points`` = some samples but below the
# threshold; ``baseline`` = a stratified detector still filling its
# multi-week lookback; ``discovery`` = a templated query we can't probe
# until discovery resolves; ``none`` = ready.
BlockedBy = Literal["none", "min_points", "lookback", "baseline", "discovery"]

# Upper bound on samples a single warm-up probe pulls per series. Keeps
# the probe cheap even for a multi-week stratified lookback: the step is
# coarsened so the range query returns at most this many points. Short
# rolling windows never reach the cap and probe at their native step.
_MAX_PROBE_POINTS = 1500


@dataclass(frozen=True)
class QueryWarmupStatus:
    """Per-query warm-up status row."""

    id: str
    status: Literal["ready", "warming", "error"]
    points_available: int
    points_needed: int
    blocked_by: str
    eta_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "points_available": self.points_available,
            "points_needed": self.points_needed,
            "blocked_by": self.blocked_by,
            "eta_seconds": self.eta_seconds,
        }


@dataclass(frozen=True)
class GroupWarmupStatus:
    """Per-group warm-up status."""

    name: str
    queries: list[QueryWarmupStatus]

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "queries": [q.to_dict() for q in self.queries]}


@dataclass(frozen=True)
class WarmupReport:
    """Full warm-up report."""

    groups: list[GroupWarmupStatus]

    def to_dict(self) -> dict[str, Any]:
        return {"groups": [g.to_dict() for g in self.groups]}


async def compute_warmup(
    *,
    config: Config,
    source: PromQLSource,
    query_timeout_seconds: float | None = None,
    now: float | None = None,
) -> WarmupReport:
    """Probe every configured query and return its warm-up status.

    Fan-out is concurrent across queries and groups. ``query_timeout_seconds``
    overrides the per-probe timeout (defaults to ``safety.query_timeout``
    if present, else the datasource timeout). Probe failures degrade the
    row to ``status: "error"`` so one bad PromQL never sinks the report.
    """
    timeout = (
        query_timeout_seconds
        if query_timeout_seconds is not None
        else config.datasource.timeout_seconds
    )
    ts = time.time() if now is None else now
    tasks = [
        _probe_group(
            config=config,
            group=group,
            source=source,
            query_timeout_seconds=timeout,
            now=ts,
        )
        for group in config.groups
    ]
    groups = await asyncio.gather(*tasks) if tasks else []
    return WarmupReport(groups=list(groups))


async def _probe_group(
    *,
    config: Config,
    group: GroupConfig,
    source: PromQLSource,
    query_timeout_seconds: float,
    now: float,
) -> GroupWarmupStatus:
    tasks = [
        _probe_query(
            config=config,
            query=query,
            source=source,
            query_timeout_seconds=query_timeout_seconds,
            now=now,
        )
        for query in group.queries
    ]
    queries = await asyncio.gather(*tasks)
    return GroupWarmupStatus(name=group.name, queries=list(queries))


async def _probe_query(
    *,
    config: Config,
    query: QueryConfig,
    source: PromQLSource,
    query_timeout_seconds: float,
    now: float,
) -> QueryWarmupStatus:
    """Probe one query and translate the result into a status row."""
    defaults = config.defaults
    step_seconds = max(defaults.step_seconds, 1.0)
    min_points = defaults.min_points

    if query.discover:
        # Templated queries can't be probed until discovery resolves the
        # rendered string — probing the raw template would just surface a
        # parse error and confuse the operator. Reported ``ready`` so the
        # CLI's exit-code loop treats it as non-blocking, but ``blocked_by``
        # makes the dependency explicit ("check the discovery counters").
        return QueryWarmupStatus(
            id=query.id,
            status="ready",
            points_available=0,
            points_needed=0,
            blocked_by="discovery",
            eta_seconds=0.0,
        )

    # A stratified detector wants a multi-week lookback before its bucket
    # baseline is meaningful. Probe over the longest window any detector
    # on the query asks for so the report reflects what the heaviest
    # detector actually needs, not just the short rolling window.
    window_seconds, needs_baseline = _probe_window_seconds(query, defaults)
    # Keep the probe cheap regardless of window length: a multi-week
    # stratified lookback at the 15s scoring step would pull >150k points
    # per series, defeating the "one cheap probe per query" promise and
    # hammering the TSDB. Coarsen the step so the response is bounded to
    # ~``_MAX_PROBE_POINTS`` samples; for the common short-window case the
    # cap never trips and the step is unchanged. ``points_needed`` is
    # measured at the same coarsened step so the available/needed fraction
    # stays a faithful "how full is the lookback" ratio.
    probe_step = step_seconds
    if window_seconds / probe_step > _MAX_PROBE_POINTS:
        probe_step = window_seconds / _MAX_PROBE_POINTS

    try:
        result = await asyncio.wait_for(
            source.range_query(
                promql=query.promql,
                end=now,
                window_seconds=window_seconds,
                step_seconds=probe_step,
                max_series=config.safety.max_series_per_query,
            ),
            timeout=query_timeout_seconds,
        )
    except TimeoutError:
        logger.warning("warmup_probe_timeout", query=query.id)
        return _error_status(query.id, min_points)
    except SourceQueryError as exc:
        logger.warning("warmup_probe_failed", query=query.id, reason=exc.reason)
        return _error_status(query.id, min_points)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("warmup_probe_failed", query=query.id, error=str(exc))
        return _error_status(query.id, min_points)

    points_available = _points_in_longest_series(result.series)

    # For a stratified detector the "enough" bar is a full lookback, not
    # ``min_points`` — surface ``baseline`` as the blocker so an operator
    # understands the wait is the multi-week fill, not a thin signal. The
    # needed-points target is computed at the (possibly coarsened) probe
    # step so it matches what ``points_available`` was measured against.
    if needs_baseline:
        needed = max(min_points, int(window_seconds / probe_step))
        if points_available < needed:
            return QueryWarmupStatus(
                id=query.id,
                status="warming",
                points_available=points_available,
                points_needed=needed,
                blocked_by="baseline" if points_available > 0 else "lookback",
                eta_seconds=_estimate_eta(points_available, needed, probe_step),
            )
        return _ready_status(query.id, points_available, needed)

    if points_available < min_points:
        return QueryWarmupStatus(
            id=query.id,
            status="warming",
            points_available=points_available,
            points_needed=min_points,
            blocked_by="min_points" if points_available > 0 else "lookback",
            eta_seconds=_estimate_eta(points_available, min_points, step_seconds),
        )
    return _ready_status(query.id, points_available, min_points)


def _probe_window_seconds(query: QueryConfig, defaults: DefaultsConfig) -> tuple[float, bool]:
    """Resolve the probe window and whether a stratified baseline applies.

    Returns ``(window_seconds, needs_baseline)``. ``needs_baseline`` is
    True when at least one detector on the query opts into the
    sliding-window stratified strategy (its lookback is what gates
    readiness, not ``min_points``).
    """
    window = defaults.window_seconds
    needs_baseline = False
    for entry in query.detectors:
        try:
            cls = get_detector(entry.name)
        except UnknownDetectorError:
            continue
        merged = merge_params(cls, entry.params, defaults)
        lookback = query_window_seconds_for(cls, merged)
        if lookback is not None and lookback > window:
            window = lookback
            needs_baseline = True
    return window, needs_baseline


def _ready_status(query_id: str, points_available: int, points_needed: int) -> QueryWarmupStatus:
    return QueryWarmupStatus(
        id=query_id,
        status="ready",
        points_available=points_available,
        points_needed=points_needed,
        blocked_by="none",
        eta_seconds=0.0,
    )


def _error_status(query_id: str, min_points: int) -> QueryWarmupStatus:
    return QueryWarmupStatus(
        id=query_id,
        status="error",
        points_available=0,
        points_needed=min_points,
        blocked_by="lookback",
        eta_seconds=math.inf,
    )


def _points_in_longest_series(series: list[Any]) -> int:
    """Length of the series with the most samples (0 when empty).

    The runner scores each series independently, so the "best off" series
    is what unblocks the query first; the mean would smear over a single
    slow-to-warm peer.
    """
    if not series:
        return 0
    return max(len(s.samples) for s in series)


def _estimate_eta(points_available: int, points_needed: int, step_seconds: float) -> float:
    """Rough seconds-to-ready estimate ("each step adds one sample").

    Underestimates for sparse series and overestimates slightly when
    backfill inserts points faster than the scrape cadence; meant to
    answer "minutes, hours, or days?", not to be a hard deadline.
    """
    if points_available >= points_needed:
        return 0.0
    return float((points_needed - points_available) * step_seconds)


def render_table(report: WarmupReport) -> str:
    """Render the report as a plain-text aligned table for the CLI."""
    if not report.groups:
        return "no groups configured\n"
    rows: list[tuple[str, ...]] = [
        ("group", "query", "status", "points", "needed", "eta", "blocked_by")
    ]
    for group in report.groups:
        for query in group.queries:
            rows.append(
                (
                    group.name,
                    query.id,
                    query.status,
                    str(query.points_available),
                    str(query.points_needed),
                    _format_eta(query.eta_seconds),
                    query.blocked_by,
                )
            )
    widths = [max(len(row[i]) for row in rows) for i in range(7)]
    lines: list[str] = []
    for i, row in enumerate(rows):
        formatted = " ".join(
            cell.ljust(widths[j]) if j in {0, 1, 2, 6} else cell.rjust(widths[j])
            for j, cell in enumerate(row)
        )
        lines.append(formatted)
        if i == 0:
            lines.append(" ".join("-" * w for w in widths))
    return "\n".join(lines) + "\n"


_SECONDS_PER_MINUTE = 60
_SECONDS_PER_HOUR = 3_600
_SECONDS_PER_DAY = 86_400


def _format_eta(eta_seconds: float) -> str:
    """Compact Prometheus-style duration for the table."""
    if not math.isfinite(eta_seconds):
        return "inf"
    if eta_seconds <= 0:
        return "-"
    seconds = round(eta_seconds)
    if seconds >= _SECONDS_PER_DAY:
        return f"{seconds // _SECONDS_PER_DAY}d"
    if seconds >= _SECONDS_PER_HOUR:
        return f"{seconds // _SECONDS_PER_HOUR}h"
    if seconds >= _SECONDS_PER_MINUTE:
        return f"{seconds // _SECONDS_PER_MINUTE}m"
    return f"{seconds}s"


def report_from_payload(payload: dict[str, Any]) -> WarmupReport:
    """Reconstruct a :class:`WarmupReport` from a JSON envelope.

    Lets the CLI reuse :func:`render_table` on the HTTP response without
    duplicating the table formatting.
    """
    groups: list[GroupWarmupStatus] = []
    for group in payload.get("groups", []):
        queries = [
            QueryWarmupStatus(
                id=str(q.get("id", "")),
                status=q.get("status", "ready"),
                points_available=int(q.get("points_available", 0)),
                points_needed=int(q.get("points_needed", 0)),
                blocked_by=str(q.get("blocked_by", "none")),
                eta_seconds=float(q.get("eta_seconds", 0.0)),
            )
            for q in group.get("queries", [])
        ]
        groups.append(GroupWarmupStatus(name=str(group.get("name", "")), queries=queries))
    return WarmupReport(groups=groups)


__all__ = [
    "BlockedBy",
    "GroupWarmupStatus",
    "QueryWarmupStatus",
    "WarmupReport",
    "compute_warmup",
    "render_table",
    "report_from_payload",
]
