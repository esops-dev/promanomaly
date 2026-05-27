"""Live introspection for one ``(id, label-selector)`` series.

Backs the ``/debug/inspect`` HTTP endpoint and the ``promanomaly
inspect`` CLI. The endpoint answers the operator's "why did this series
score 4.7?" question without restarting or attaching a debugger.

Implementation
--------------
The detector is stateless across restarts, so the rolling-window data
isn't held in memory — it comes from the TSDB. We refetch the
configured PromQL for the target id, filter by the operator-supplied
label selector, and re-run each configured detector against the live
window. Per-detector outputs (score, baseline, bands, change-point
flag), the warm-up counter from the snapshot store, and the cached
calibration data (when present) round out the picture.

This is **not** a cache lookup — it's a fresh detector pass exposed
through HTTP. Two consecutive ``/debug/inspect`` calls can return
different scores if the TSDB picked up a new sample between them. That
is the correct semantics: operators inspecting "right now" want the
current view, not the snapshot from the last scheduled run.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from .config import Config, DefaultsConfig, GroupConfig, QueryConfig
from .detectors import UnknownDetectorError, get
from .runner import _build_detector
from .source import PromQLSource, QuerySeries, SourceQueryError
from .state import SnapshotStore


class InspectError(Exception):
    """Inspect-specific failures with a short, machine-readable reason."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(f"{reason}: {message}")
        self.reason = reason
        self.message = message


# Cap the rolling window we include in the response so a 1h@15s window
# (240 samples) returns the full window, but a stratified detector that
# pulled 7d of data doesn't ship a 40k-sample JSON blob over HTTP.
_MAX_WINDOW_SAMPLES: int = 500


@dataclass(frozen=True)
class _ResolvedQuery:
    group: GroupConfig
    query: QueryConfig


def parse_label_selector(value: str | None) -> dict[str, str]:
    """Parse ``"k1=v1,k2=v2"`` style label selectors.

    Returns an empty dict for ``None`` or the empty string — meaning
    "match anything", consistent with how ``promtool query`` treats an
    empty matcher.
    """
    if not value:
        return {}
    selector: dict[str, str] = {}
    for raw in value.split(","):
        raw = raw.strip()
        if not raw:
            continue
        if "=" not in raw:
            raise InspectError(
                "bad_selector",
                f"label selector entry {raw!r} must be key=value",
            )
        key, _, val = raw.partition("=")
        key = key.strip()
        val = val.strip()
        if not key:
            raise InspectError("bad_selector", f"label selector entry {raw!r}: empty key")
        selector[key] = val
    return selector


def _resolve(config: Config, metric_id: str) -> _ResolvedQuery:
    """Find the (group, query) whose ``id`` matches ``metric_id``.

    Discovery-templated queries are not yet resolvable here — the
    operator inspects rendered ids (e.g. ``cpu_busy_node_17``) and the
    config carries the un-rendered template (``cpu_busy_{{ instance }}``).
    Surfacing them would require replaying the discovery probe, which
    is a future enhancement.

    We deliberately distinguish "no match at all" from "matched a
    discover-templated query" so the CLI can surface an actionable
    hint pointing the operator at ``detect-once`` for ad-hoc inspection
    of discovery-expanded series.
    """
    discover_match: _ResolvedQuery | None = None
    for group in config.groups:
        for query in group.queries:
            if query.id != metric_id:
                continue
            if query.discover:
                discover_match = _ResolvedQuery(group=group, query=query)
                continue
            return _ResolvedQuery(group=group, query=query)
    if discover_match is not None:
        raise InspectError(
            "discovery_templated",
            (
                f"query id={metric_id!r} is a discover: template; inspect "
                "doesn't yet resolve rendered variants. Use "
                "`promanomaly detect-once --query '<rendered promql>'` "
                "for ad-hoc inspection of one discovered series."
            ),
        )
    raise InspectError("unknown_id", f"no group/query found with id={metric_id!r}")


def _matches(series: QuerySeries, selector: dict[str, str]) -> bool:
    return all(series.labels.get(key) == value for key, value in selector.items())


def _trim_window(series: QuerySeries) -> list[dict[str, float]]:
    """Return the trailing ``_MAX_WINDOW_SAMPLES`` of a series for the response."""
    df = series.samples
    if len(df) > _MAX_WINDOW_SAMPLES:
        df = df.tail(_MAX_WINDOW_SAMPLES)
    # ``itertuples`` returns NamedTuples typed as the broader pandas
    # column union; coerce through str()/float() to a narrow ``float``
    # for the JSON payload without tripping mypy on the union type.
    timestamps = df["timestamp"].to_numpy(dtype=float, copy=False)
    ys = df["y"].to_numpy(dtype=float, copy=False)
    return [{"timestamp": float(t), "y": float(y)} for t, y in zip(timestamps, ys, strict=True)]


def _detector_input_window_seconds(cls: Any, params: dict[str, Any], defaults: float) -> float:
    """Mirror Runner._detector_input_window_seconds for inspect-time scoring."""
    override = getattr(cls, "query_window_seconds", None)
    if callable(override):
        value = override(params)
        if value is not None:
            return float(value)
    return defaults


async def inspect_series(
    *,
    config: Config,
    source: PromQLSource,
    store: SnapshotStore,
    metric_id: str,
    label_selector: str | None = None,
) -> dict[str, Any]:
    """Build the ``/debug/inspect`` response for one (id, label-selector).

    Re-queries the source for the configured PromQL, filters by the
    selector, and runs every configured detector against the live
    window. Calibration data and warm-up state come from the snapshot
    store. Returns an error dict (status="error") rather than raising
    when the source query fails; the endpoint always responds with the
    same JSON shape so dashboards can poll it without special-casing.
    """
    selector = parse_label_selector(label_selector)
    resolved = _resolve(config, metric_id)
    group = resolved.group
    query = resolved.query
    defaults: DefaultsConfig = config.defaults

    # Build the per-detector plan up front so calibration lookups and
    # detector runs can share one validated plan.
    plans: list[dict[str, Any]] = []
    desired_window = defaults.window_seconds
    for entry in query.detectors:
        try:
            cls = get(entry.name)
        except UnknownDetectorError as exc:
            raise InspectError("unknown_detector", str(exc)) from exc
        merged: dict[str, Any] = {}
        merged.update(getattr(cls, "defaults", {}) or {})
        merged.update(entry.params or {})
        merged.setdefault("window", defaults.window)
        merged.setdefault("step", defaults.step)
        plans.append(
            {
                "name": entry.name,
                "instance": entry.instance,
                "cls": cls,
                "params": merged,
            }
        )
        det_window = _detector_input_window_seconds(cls, merged, defaults.window_seconds)
        if det_window > desired_window:
            desired_window = det_window

    end = time.time()
    try:
        qresult = await source.range_query(
            promql=query.promql,
            end=end,
            window_seconds=desired_window,
            step_seconds=defaults.step_seconds,
            max_series=config.safety.max_series_per_query,
        )
    except SourceQueryError as exc:
        return {
            "status": "error",
            "reason": exc.reason,
            "detail": exc.message,
            "id": metric_id,
            "group": group.name,
            "query": query.promql,
            "label_selector": selector,
        }

    matching = [s for s in qresult.series if _matches(s, selector)]
    series_payload: list[dict[str, Any]] = []
    for series in matching:
        sample_count = len(series.samples)
        warm = sample_count < defaults.min_points
        observed_runs = store.observed_runs(group.name, query.id, series.label_key)
        per_detector: list[dict[str, Any]] = []
        for plan in plans:
            cls = plan["cls"]
            params = plan["params"]
            instance: str | None = plan["instance"]
            name: str = plan["name"]
            calibration = store.get_calibration(
                group.name, query.id, series.label_key, name, instance
            )
            det_entry: dict[str, Any] = {
                "name": name,
                "instance": instance,
                "params": {k: v for k, v in params.items() if k not in {"window", "step"}},
                "window": params.get("window"),
                "step": params.get("step"),
                "calibration": calibration.as_dict() if calibration is not None else None,
            }
            if warm:
                det_entry["status"] = "warming_up"
                per_detector.append(det_entry)
                continue
            # Mirror the runner's slicing so the detector sees the same
            # window it would see during a scheduled run.
            det_window = _detector_input_window_seconds(cls, params, defaults.window_seconds)
            latest_ts = float(series.samples["timestamp"].iloc[-1])
            cutoff = latest_ts - det_window
            sliced = series.samples[series.samples["timestamp"] >= cutoff]
            if sliced.empty:
                sliced = series.samples.tail(1).reset_index(drop=True)
            try:
                detector = _build_detector(cls, params)
                df = detector.fit_score(sliced.reset_index(drop=True), "", params)
            except Exception as exc:
                det_entry["status"] = "error"
                det_entry["error"] = str(exc)
                per_detector.append(det_entry)
                continue
            row = df.iloc[-1]
            fallback = (query.alert_thresholds or defaults.alert_thresholds).score
            # Same resolution rule the runner uses: a detector that
            # handles its own threshold semantics surfaces a ``threshold``
            # param (BOCPD's probability score) so its alerting bit does
            # not get compared against the group's sigma-multiplier knob.
            threshold_raw = params.get("threshold")
            if threshold_raw is None:
                threshold = float(fallback)
            else:
                try:
                    threshold = float(threshold_raw)
                except (TypeError, ValueError):
                    threshold = float(fallback)
            score = float(row.get("score", 0.0))
            baseline = float(row.get("baseline", 0.0))
            det_entry["status"] = "ok"
            det_entry["score"] = score
            det_entry["baseline"] = baseline
            det_entry["threshold"] = float(threshold)
            # ``is_outside`` reflects the detector's own threshold semantics
            # (BOCPD's probability score, future change-point detectors).
            # The runner ORs it with the statistical check; here we surface
            # both signals separately so operators can see which one fired.
            det_entry["is_outside"] = bool(row.get("is_outside", False))
            det_entry["statistical_outside"] = score >= threshold
            if "baseline_upper" in df.columns:
                det_entry["baseline_upper"] = float(row["baseline_upper"])
            if "baseline_lower" in df.columns:
                det_entry["baseline_lower"] = float(row["baseline_lower"])
            if "change_point" in df.columns:
                det_entry["change_point"] = bool(row["change_point"])
            per_detector.append(det_entry)

        series_payload.append(
            {
                "labels": dict(series.labels),
                "sample_count": sample_count,
                "latest_timestamp": float(series.samples["timestamp"].iloc[-1]),
                "latest_y": float(series.samples["y"].iloc[-1]),
                "warming_up": warm,
                "warmup_observed_runs": observed_runs,
                "warmup_min_points": defaults.min_points,
                "window": _trim_window(series),
                "detectors": per_detector,
            }
        )

    return {
        "status": "ok",
        "id": metric_id,
        "group": group.name,
        "query": query.promql,
        "label_selector": selector,
        "warmup_policy": defaults.warmup_policy,
        "matched_series": len(series_payload),
        "total_series": len(qresult.series),
        "series": series_payload,
    }


__all__ = ["InspectError", "inspect_series", "parse_label_selector"]
