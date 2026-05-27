"""Prometheus exposition + the metric-name validator.

Snapshot metrics (``anomaly_score``, ``anomaly_baseline``, ...) have
per-series labels copied from the source query, so the cardinality of
the label set is not knowable in advance. The standard
``prometheus_client.Gauge`` API requires a fixed labelnames list, which
is the wrong shape for our output. We therefore render snapshot lines
manually and concatenate them with the operational registry's standard
output.

Operational metrics (``anomaly_last_run_timestamp_seconds``,
``anomaly_failures_total``, ...) have bounded labels and live in a
private :class:`CollectorRegistry`.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)

from .source import PromQLSource
from .state import Sample, SnapshotStore

# Same character class Prometheus enforces on metric and label names.
# We additionally forbid colons — they're reserved for recording rules,
# and slipping one into our output makes the output collide with users'
# recording rule namespace. The project guide is explicit about this.
_METRIC_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_LABEL_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class InvalidMetricNameError(ValueError):
    """Raised when output code attempts to emit a name that violates the
    promanomaly metric-naming rules. The exporter is the last line of
    defence — by the time we're here the registry / runner have failed
    to catch the offender upstream."""


def validate_metric_name(name: str) -> str:
    """Reject metric names with colons or otherwise-invalid characters."""
    if ":" in name:
        raise InvalidMetricNameError(
            f"metric name {name!r} contains ':' — colons are reserved for recording rules"
        )
    if not _METRIC_NAME_RE.match(name):
        raise InvalidMetricNameError(
            f"metric name {name!r} does not match {_METRIC_NAME_RE.pattern}"
        )
    return name


def validate_label_name(name: str) -> str:
    if ":" in name:
        raise InvalidMetricNameError(
            f"label name {name!r} contains ':' — colons are reserved for recording rules"
        )
    if not _LABEL_NAME_RE.match(name):
        raise InvalidMetricNameError(f"label name {name!r} does not match {_LABEL_NAME_RE.pattern}")
    return name


# ----------------------------------------------------------------------
# Output metric documentation, used in the # HELP lines so dashboards
# pick up sensible descriptions when introspecting `/metrics`.
# ----------------------------------------------------------------------

_HELP: dict[str, tuple[str, str]] = {
    # name -> (type, help-text)
    "anomaly_score": (
        "gauge",
        "Raw anomaly score for the latest sample (units: number of MADs for MAD/Hampel).",
    ),
    "anomaly_outside_threshold": (
        "gauge",
        "1 when the score exceeds alert_thresholds.score, else 0.",
    ),
    "anomaly_baseline": (
        "gauge",
        "Detector baseline value (rolling median for MAD, local median for Hampel).",
    ),
    "anomaly_warming_up": (
        "gauge",
        "1 while the rolling window is still filling toward min_points.",
    ),
    "anomaly_signal_missing": (
        "gauge",
        "1 when an expect:true query has returned no series for expect_grace_runs runs.",
    ),
    "anomaly_duration_seconds": (
        "gauge",
        "Seconds since anomaly_outside_threshold last transitioned 0 -> 1; 0 when clear.",
    ),
    "anomaly_confidence_score": (
        "gauge",
        "Per-detector confidence (0-1) from the synthetic-injection calibration cycle.",
    ),
    "anomaly_baseline_stability": (
        "gauge",
        "Per-detector baseline stability (0-1, higher = more stable) from calibration.",
    ),
    "anomaly_composite_score": (
        "gauge",
        (
            "Ensemble-fused score across the detectors in the group "
            "(max of per-detector scores). Emitted only when an "
            "ensemble: block is configured."
        ),
    ),
    "anomaly_composite_outside_threshold": (
        "gauge",
        (
            "1 when the configured ensemble method (max | voting) fires, "
            "else 0. Emitted only when an ensemble: block is configured."
        ),
    ),
    "anomaly_change_point_total": (
        "counter",
        (
            "Cumulative change-points observed by a change-point detector "
            "(BOCPD, CUSUM) for one (id, group, detector) series. Resets "
            "to zero on process restart — Prometheus's counter-reset "
            "handling absorbs that."
        ),
    ),
}


def help_for(name: str) -> tuple[str, str]:
    """Return ``(type, help)`` for an output metric, with safe defaults."""
    return _HELP.get(name, ("gauge", ""))


def render_label_value(value: str) -> str:
    """Escape a label value per the Prometheus exposition format."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _format_sample(sample: Sample) -> str:
    """One ``metric{labels} value`` line."""
    if sample.labels:
        label_pairs = ",".join(f'{k}="{render_label_value(v)}"' for k, v in sample.labels)
        return f"{sample.metric}{{{label_pairs}}} {sample.value}"
    return f"{sample.metric} {sample.value}"


def render_samples(samples: Iterable[Sample]) -> str:
    """Render snapshot samples into Prometheus exposition format.

    Validates the metric name and every label name on the way through —
    a bug in a detector that produces a colon-containing name is caught
    here rather than poisoning downstream scrapes.
    """
    by_metric: dict[str, list[Sample]] = defaultdict(list)
    for sample in samples:
        validate_metric_name(sample.metric)
        for label_name, _ in sample.labels:
            validate_label_name(label_name)
        by_metric[sample.metric].append(sample)

    lines: list[str] = []
    for metric_name in sorted(by_metric):
        metric_type, help_text = help_for(metric_name)
        if help_text:
            lines.append(f"# HELP {metric_name} {help_text}")
        lines.append(f"# TYPE {metric_name} {metric_type}")
        for sample in by_metric[metric_name]:
            lines.append(_format_sample(sample))
    return "\n".join(lines) + ("\n" if lines else "")


class OperationalMetrics:
    """Bounded-cardinality operational metrics, served from a private registry.

    Kept apart from snapshot metrics so reload-driven group churn never
    leaks high-cardinality samples through prometheus_client.
    """

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.last_run_timestamp_seconds = Gauge(
            "anomaly_last_run_timestamp_seconds",
            "Wall-clock timestamp of the last successful group run.",
            ["group"],
            registry=self.registry,
        )
        self.run_duration_seconds = Gauge(
            "anomaly_run_duration_seconds",
            "Wall-clock duration of the last group run, in seconds.",
            ["group"],
            registry=self.registry,
        )
        self.series_count = Gauge(
            "anomaly_series_count",
            "Number of series produced by the last successful group run.",
            ["group"],
            registry=self.registry,
        )
        self.group_ready = Gauge(
            "anomaly_group_ready",
            (
                "1 when the group has a fresh successful snapshot, 0 otherwise. "
                "Under safety.on_source_failure=fail_ready this flips back to 0 "
                "after safety.fail_ready_after consecutive source-failed runs and "
                "recovers on the next successful run; under serve_stale and "
                "drop_scores the gauge is monotonic once a first successful run "
                "has landed."
            ),
            ["group"],
            registry=self.registry,
        )
        self.source_failure_streak = Gauge(
            "anomaly_source_failure_streak",
            (
                "Consecutive source-failed runs for the group. Only meaningful "
                "under safety.on_source_failure=fail_ready, where it crosses "
                "safety.fail_ready_after just before anomaly_group_ready trips."
            ),
            ["group"],
            registry=self.registry,
        )
        self.config_hash = Gauge(
            "anomaly_config_hash",
            "Hash of the loaded configuration (label-only).",
            ["sha256"],
            registry=self.registry,
        )
        self.failures_total = Counter(
            "anomaly_failures_total",
            "Total per-series detector failures bucketed by reason.",
            ["group", "detector", "reason"],
            registry=self.registry,
        )
        self.source_failure_total = Counter(
            "anomaly_source_failure_total",
            "Total source-query (TSDB) failures bucketed by reason.",
            ["group", "reason"],
            registry=self.registry,
        )
        self.signal_missing_total = Counter(
            "anomaly_signal_missing_total",
            "Total observed transitions into 'expected but absent'.",
            ["group", "reason"],
            registry=self.registry,
        )
        self.series_dropped_total = Counter(
            "anomaly_series_dropped_total",
            "Series trimmed by cardinality controls (per reason).",
            ["group", "reason"],
            registry=self.registry,
        )
        self.detect_duration_seconds = Gauge(
            "anomaly_detect_duration_seconds",
            "Wall-clock time spent in detector compute per group/detector.",
            ["group", "detector"],
            registry=self.registry,
        )
        # Query-cache observability. Cardinality is bounded — one set of
        # series per cache (single instance per process today; the v0.4
        # Redis variant will publish the same metric family without
        # changing dashboards).
        self.query_cache_hits_total = Counter(
            "anomaly_query_cache_hits_total",
            "PromQL responses served from the in-process query cache.",
            registry=self.registry,
        )
        self.query_cache_misses_total = Counter(
            "anomaly_query_cache_misses_total",
            "PromQL responses that missed the cache and hit the TSDB.",
            registry=self.registry,
        )
        self.query_cache_evictions_total = Counter(
            "anomaly_query_cache_evictions_total",
            "Cache entries dropped by LRU eviction (max_entries exceeded).",
            registry=self.registry,
        )
        self.query_cache_expirations_total = Counter(
            "anomaly_query_cache_expirations_total",
            "Cache entries dropped by TTL expiry on lookup.",
            registry=self.registry,
        )
        self.query_cache_entries = Gauge(
            "anomaly_query_cache_entries",
            "Number of entries currently held in the in-process query cache.",
            registry=self.registry,
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)


class Exporter:
    """Glue between the snapshot store, the operational registry, and HTTP."""

    def __init__(
        self,
        store: SnapshotStore,
        operational: OperationalMetrics,
        source: PromQLSource | None = None,
    ) -> None:
        self._store = store
        self._operational = operational
        self._source = source
        # Counter snapshots used to map cache stats (absolute counters
        # maintained inside the cache) onto prometheus_client Counters
        # (which only expose .inc()). We forward the delta on each
        # render so the wire output is monotonic. ``_cache_id`` lets us
        # detect when the cache instance has been replaced by a reload
        # (``replace_cache`` swaps in a fresh TTLCache); without it the
        # snapshot would lag behind, treating the new cache's growing
        # stats as deltas relative to the old cache's final totals.
        self._cache_id: int | None = None
        self._cache_stat_snapshot: dict[str, int] = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "expirations": 0,
        }

    def render(self) -> tuple[bytes, str]:
        self._refresh_cache_metrics()
        op_bytes = self._operational.render()
        snapshot_text = render_samples(self._store.all_samples())
        body = op_bytes + snapshot_text.encode("utf-8")
        return body, CONTENT_TYPE_LATEST

    def _refresh_cache_metrics(self) -> None:
        if self._source is None or self._source.cache is None:
            return
        cache = self._source.cache
        if id(cache) != self._cache_id:
            # Reload swapped the cache out. The previous cache's totals
            # were already inc'd onto the counters and stay there; the
            # new cache restarts at 0, so reset the snapshot too —
            # otherwise the next several renders skip updates while the
            # new cache "catches up" to the stale snapshot.
            self._cache_id = id(cache)
            self._cache_stat_snapshot = {
                "hits": 0,
                "misses": 0,
                "evictions": 0,
                "expirations": 0,
            }
        stats = cache.stats
        for key, counter in (
            ("hits", self._operational.query_cache_hits_total),
            ("misses", self._operational.query_cache_misses_total),
            ("evictions", self._operational.query_cache_evictions_total),
            ("expirations", self._operational.query_cache_expirations_total),
        ):
            current = getattr(stats, key)
            previous = self._cache_stat_snapshot[key]
            delta = current - previous
            if delta > 0:
                counter.inc(delta)
                self._cache_stat_snapshot[key] = current
        self._operational.query_cache_entries.set(float(len(cache)))


__all__ = [
    "CONTENT_TYPE_LATEST",
    "Exporter",
    "InvalidMetricNameError",
    "OperationalMetrics",
    "help_for",
    "render_label_value",
    "render_samples",
    "validate_label_name",
    "validate_metric_name",
]
