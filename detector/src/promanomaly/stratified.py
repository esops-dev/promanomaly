"""Shared primitives for the sliding-window stratified-fetch strategy.

Both the runner (live execution) and the CLI (``validate --probe`` cost
projection) need to ask the same questions about a detector class:

- Does it opt into the sliding-window strategy?
- What baseline window does it want?
- How often may that baseline be reused?

Keeping the questions in one place avoids the cli / runner drift that
would otherwise creep in when a new detector adopts the strategy via a
different mechanism.

Detectors opt in by exposing two classmethods:

- ``query_window_seconds(params) -> float | None``
- ``baseline_refresh_interval_seconds(params) -> float | None``

A detector that returns ``None`` (or omits either method) falls back to
the per-run full-window fetch path — same behaviour as a plain rolling
detector. Both methods must return a value for the strategy to activate.
"""

from __future__ import annotations

from typing import Any, Protocol

BASELINE_REFRESH_ATTR = "baseline_refresh_interval_seconds"
QUERY_WINDOW_ATTR = "query_window_seconds"


class _ResolveCls(Protocol):
    """Callable that maps a detector name to its class (or raises)."""

    def __call__(self, name: str) -> type[Any]: ...


def query_window_seconds_for(cls: type[Any], params: dict[str, Any]) -> float | None:
    """Resolve ``query_window_seconds`` for a detector class, defensively.

    Returns ``None`` when the detector doesn't expose the method, when
    the call raises, or when the resolved value is ``<= 0`` — callers
    fall back to the per-run full-window path. Exceptions are
    swallowed because this is hot-path code; a buggy detector should
    not crash the whole run.

    The non-positive guard mirrors :func:`baseline_refresh_seconds_for`:
    ``lookback: 0`` would otherwise issue a zero-second TSDB range
    query, return empty data, and silently collapse the bucket
    baseline — the operator-confusing failure mode the boot-time
    warnings exist to surface.
    """
    raw = query_window_seconds_raw_for(cls, params)
    if raw is None:
        return None
    if raw <= 0.0:
        return None
    return raw


def query_window_seconds_raw_for(cls: type[Any], params: dict[str, Any]) -> float | None:
    """Raw read of ``query_window_seconds``, no value gating.

    Symmetric with :func:`baseline_refresh_seconds_raw_for` — used by
    the misconfig collector to surface a bad value instead of silently
    treating it as "not stratified".
    """
    fn = getattr(cls, QUERY_WINDOW_ATTR, None)
    if not callable(fn):
        return None
    try:
        value = fn(params)
    except Exception:
        return None
    return float(value) if value is not None else None


def baseline_refresh_seconds_for(cls: type[Any], params: dict[str, Any]) -> float | None:
    """Resolve ``baseline_refresh_interval_seconds`` for a detector class.

    Same defensive shape as :func:`query_window_seconds_for`.
    Additionally rejects non-positive values: setting
    ``baseline_refresh_interval: 0`` or negative would either always
    re-fetch (cache thrash) or never re-fetch (frozen baseline) — both
    defeat the strategy. Logging the bad value is left to the caller so
    this helper stays pure.
    """
    raw = baseline_refresh_seconds_raw_for(cls, params)
    if raw is None:
        return None
    if raw <= 0.0:
        return None
    return raw


def baseline_refresh_seconds_raw_for(cls: type[Any], params: dict[str, Any]) -> float | None:
    """Raw read of ``baseline_refresh_interval_seconds``, no value gating.

    Used by config-warning code that needs to surface a *bad* value
    (negative, zero, below refresh_interval) instead of silently
    treating it as "not stratified". Production fetch paths should
    keep using :func:`baseline_refresh_seconds_for` which gates
    non-positive values to None.
    """
    fn = getattr(cls, BASELINE_REFRESH_ATTR, None)
    if not callable(fn):
        return None
    try:
        value = fn(params)
    except Exception:
        return None
    if value is None:
        return None
    return float(value)


def stratified_cost_summary(
    query: Any,
    resolve_cls: _ResolveCls,
    refresh_interval_seconds: float,
) -> dict[str, Any] | None:
    """Project per-day TSDB cost for stratified detectors on ``query``.

    Returns ``None`` when no detector on the query opts into the
    sliding-window strategy. When at least one does, returns a dict
    with the aggregated baseline window / refresh cadence and the
    per-detector breakdown — the surface ``validate --probe`` prints
    so an operator reviewing a PR can spot a runaway
    ``baseline_refresh_interval: 1m`` before it lands.

    Pure metadata: invokes detector classmethods only, never hits the
    TSDB.
    """
    short_per_day = 86400.0 / refresh_interval_seconds if refresh_interval_seconds > 0 else 0.0
    detector_rows: list[dict[str, Any]] = []
    baseline_refresh: float | None = None
    baseline_window: float | None = None
    for entry in query.detectors:
        try:
            cls = resolve_cls(entry.name)
        except Exception:
            # Unknown detector — schema validation should have flagged
            # this, but keep the probe path defensive.
            continue
        merged = dict(getattr(cls, "defaults", {}) or {})
        merged.update(entry.params or {})
        window_val = query_window_seconds_for(cls, merged)
        refresh_val = baseline_refresh_seconds_for(cls, merged)
        if window_val is None or refresh_val is None:
            continue
        baseline_per_day = 86400.0 / refresh_val if refresh_val > 0 else 0.0
        detector_rows.append(
            {
                "name": entry.name,
                "instance": entry.instance,
                "baseline_window_seconds": window_val,
                "baseline_refresh_seconds": refresh_val,
                "baseline_queries_per_day": round(baseline_per_day, 2),
            }
        )
        if baseline_window is None or window_val > baseline_window:
            baseline_window = window_val
        if baseline_refresh is None or refresh_val < baseline_refresh:
            baseline_refresh = refresh_val
    if not detector_rows or baseline_window is None or baseline_refresh is None:
        return None
    baseline_per_day = 86400.0 / baseline_refresh if baseline_refresh > 0 else 0.0
    return {
        "baseline_window_seconds": baseline_window,
        "baseline_refresh_seconds": baseline_refresh,
        "refresh_interval_seconds": refresh_interval_seconds,
        "baseline_queries_per_day": round(baseline_per_day, 2),
        "short_queries_per_day": round(short_per_day, 2),
        "detectors": detector_rows,
    }


def collect_misconfigurations(
    groups: Any,
    resolve_cls: _ResolveCls,
    refresh_interval_seconds: float,
) -> list[dict[str, Any]]:
    """Return a list of stratified-config issues worth warning about.

    Three categories:

    - ``invalid_value`` — ``baseline_refresh_interval`` resolved to a
      non-positive number. The strategy degrades to per-run full-window
      fetches (the runner sees ``None`` from
      :func:`baseline_refresh_seconds_for` and falls back).
    - ``below_refresh_interval`` — ``baseline_refresh_interval`` is
      positive but shorter than ``server.refresh_interval``. The cache
      is always stale on the next refresh, so the heavy long-history
      query fires every refresh anyway — defeats the strategy without
      crashing.
    - ``invalid_lookback`` — ``query_window_seconds`` resolved to a
      non-positive number. A zero-second TSDB range query returns
      empty data and the bucket baseline silently collapses; the
      strategy degrades to per-run fetches.

    Caller is expected to log each entry once at boot / reload. Pure
    function; no logging here so the helper stays test-friendly.
    """
    findings: list[dict[str, Any]] = []
    for group in groups:
        for query in group.queries:
            for entry in query.detectors:
                try:
                    cls = resolve_cls(entry.name)
                except Exception:
                    continue
                merged = dict(getattr(cls, "defaults", {}) or {})
                merged.update(entry.params or {})
                base = {
                    "group": group.name,
                    "query": query.id,
                    "detector": entry.name,
                    "instance": entry.instance,
                }
                refresh_raw = baseline_refresh_seconds_raw_for(cls, merged)
                window_raw = query_window_seconds_raw_for(cls, merged)
                if refresh_raw is not None:
                    if refresh_raw <= 0.0:
                        findings.append(
                            {
                                **base,
                                "kind": "invalid_value",
                                "baseline_refresh_seconds": refresh_raw,
                            }
                        )
                    elif refresh_raw < refresh_interval_seconds:
                        findings.append(
                            {
                                **base,
                                "kind": "below_refresh_interval",
                                "baseline_refresh_seconds": refresh_raw,
                                "refresh_interval_seconds": refresh_interval_seconds,
                            }
                        )
                if window_raw is not None and window_raw <= 0.0:
                    findings.append(
                        {
                            **base,
                            "kind": "invalid_lookback",
                            "query_window_seconds": window_raw,
                        }
                    )
    return findings


__all__ = [
    "BASELINE_REFRESH_ATTR",
    "QUERY_WINDOW_ATTR",
    "baseline_refresh_seconds_for",
    "baseline_refresh_seconds_raw_for",
    "collect_misconfigurations",
    "query_window_seconds_for",
    "query_window_seconds_raw_for",
    "stratified_cost_summary",
]
