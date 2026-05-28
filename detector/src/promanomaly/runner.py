"""Per-group, per-series detection pipeline.

Wraps every layer in try/except so a single bad PromQL / runaway
detector / poisoned series cannot take down a group:

    group  ->  query  ->  series  ->  detector

A failed series leaves the previous successful snapshot for that series
untouched (the runner produces a fresh snapshot per group, so the entire
last good snapshot is replaced atomically on success). A failed group
emits nothing for that run and bumps the failure counter.

Per-series timeout uses ``concurrent.futures`` cancellation, governed
by ``safety.detect_timeout`` — runaway detectors do not block the rest
of the queue.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import os
import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
import structlog

from .calibration import DetectorCalibration, calibrate_detector
from .config import (
    Config,
    DefaultsConfig,
    DetectorEntry,
    EnsembleConfig,
    GroupConfig,
    QueryConfig,
)
from .detectors import Detector, UnknownDetectorError, get
from .detectors.cohort import (
    _PARAM_COHORT_MAD,
    _PARAM_COHORT_MEDIAN,
    _PARAM_COHORT_SIZE,
    cohort_key_for,
    compute_cohort_baseline,
    is_cohort_aware,
)
from .discovery import (
    DiscoveryError,
    ExpandedQuery,
    expand_query,
    probe_variable,
    render_template,
)
from .exporter import OperationalMetrics
from .source import PromQLSource, QueryResult, QuerySeries, SourceQueryError
from .state import GroupSnapshot, Sample, SnapshotStore
from .stratified import (
    baseline_refresh_seconds_for,
    collect_misconfigurations,
    query_window_seconds_for,
)
from .telemetry import Telemetry

logger = structlog.get_logger(__name__)


# ----------------------------------------------------------------------
# Reason taxonomy for anomaly_failures_total / anomaly_source_failure_total.
# Keep this tight — anomaly_failures_total{reason=...} cardinality is the
# whole point of the bound. Adding a new value is a public-API decision.
# ----------------------------------------------------------------------
REASON_TIMEOUT = "timeout"
REASON_DETECTOR_EXCEPTION = "detector_exception"
REASON_INSUFFICIENT_DATA = "insufficient_data"
REASON_UNKNOWN_DETECTOR = "unknown_detector"
REASON_QUERY_EXCEPTION = "query_exception"
REASON_CARDINALITY = "cardinality_overflow"


@dataclass
class _DetectorPlan:
    name: str
    instance: str | None
    cls: type[Detector]
    params: dict[str, Any]


@dataclass
class _DetectorOutcome:
    """Per-detector score + alerting bit for one series.

    Captured per series so the ensemble fuser (when configured) can
    fold the individual detector verdicts into a single composite
    score without re-running detectors. The runner only retains these
    long enough to compute composites and discards them.
    """

    plan: _DetectorPlan
    score: float
    outside: bool


@dataclass
class _GroupInnerResult:
    samples: list[Sample] = field(default_factory=list)
    series_count: int = 0
    dropped: dict[str, int] = field(default_factory=dict)
    # Number of queries that hit a SourceQueryError (drop_scores only —
    # other policies raise on the first such failure and never reach
    # this counter).
    source_failures: int = 0
    # Number of queries whose fetch completed without raising. A query
    # that fetched successfully but returned zero series still counts
    # here — the run produced fresh information about that query.
    queries_succeeded: int = 0


@dataclass(frozen=True)
class _CohortBaseline:
    """Per-cohort cross-member median + MAD precomputed by the runner.

    Built once per ``(plan, cohort_key)`` before scoring loops over the
    cohort's members so every member sees the same baseline. Stored
    frozen because the runner threads it through several layers and
    accidental mutation would silently corrupt subsequent members'
    scores.
    """

    cohort_median: float
    cohort_mad: float
    size: int


@dataclass
class _CohortContext:
    """All cohort baselines for one query run, indexed by plan + cohort key.

    Cohort-aware plans look up their per-series baseline via
    ``baselines[(plan.name, plan.instance), cohort_key]``. ``label_by_plan``
    records the resolved ``cohort_label`` per plan so the runner can stamp
    the opt-in ``cohort_label`` output label on emitted samples.
    """

    baselines: dict[
        tuple[tuple[str, str | None], tuple[tuple[str, str], ...]],
        _CohortBaseline,
    ] = field(default_factory=dict)
    label_by_plan: dict[tuple[str, str | None], str] = field(default_factory=dict)


@dataclass
class _StratifiedBaseline:
    """Cached long-history fetch for one (group, query) under the
    sliding-window strategy.

    The runner caches the heavy multi-week baseline pull and only
    refreshes it at ``baseline_refresh_interval`` cadence; every detector
    run still pulls the short window fresh and stitches it onto this
    cache before scoring. Keyed by (group, rendered_id); rebuilt from
    zero on restart and on config reload (state for removed groups is
    dropped by :meth:`Runner.replace_config`).

    ``promql`` is captured so a reload that changes the PromQL string of
    a kept ``(group, query_id)`` invalidates the cache — otherwise the
    runner would silently stitch the old signal's baseline onto the new
    signal's short window.

    ``template_query_id`` and ``template_promql`` carry the
    pre-substitution form for discover-templated queries (and equal the
    rendered form for non-discover queries). Reload identity is checked
    against the template, not the rendered form, so reloading a
    discover template's PromQL invalidates all its rendered descendants
    at once — and reloading without any change preserves them.
    """

    fetched_at: float
    promql: str
    window_seconds: float
    step_seconds: float
    series_by_key: dict[tuple[tuple[str, str], ...], QuerySeries]
    template_query_id: str = ""
    template_promql: str = ""
    truncated: bool = False


@dataclass
class GroupRunResult:
    group: str
    samples: list[Sample] = field(default_factory=list)
    duration_seconds: float = 0.0
    series_count: int = 0
    dropped: dict[str, int] = field(default_factory=dict)
    succeeded: bool = False
    # Number of queries that hit a SourceQueryError this run. Only
    # populated when ``safety.on_source_failure=drop_scores`` since
    # the other policies abort the run on the first source failure.
    source_failures: int = 0
    # Number of queries that produced at least one fetch result.
    queries_succeeded: int = 0


class Runner:
    """Owns the detection pipeline shared across all groups.

    Single instance per process. Methods are async because the source
    client is async; the actual detector compute runs in a thread pool so
    a slow detector doesn't stall the event loop.
    """

    def __init__(
        self,
        config: Config,
        source: PromQLSource,
        store: SnapshotStore,
        operational: OperationalMetrics,
        *,
        executor: concurrent.futures.ThreadPoolExecutor | None = None,
        telemetry: Telemetry | None = None,
    ) -> None:
        self._config = config
        self._source = source
        self._store = store
        self._ops = operational
        # OpenTelemetry tracing is optional and zero-overhead when disabled.
        # ``Telemetry.disabled()`` returns a singleton with no-op spans so
        # callers can ``with self._telemetry.span(...)`` unconditionally.
        self._telemetry = telemetry or Telemetry.disabled()
        # CPU-bound detector work runs in this pool. Sized to ``nproc``
        # with a floor of 2 so dev boxes always have headroom; user-
        # supplied executors override the default for tests.
        cpu = os.cpu_count() or 2
        self._executor = executor or concurrent.futures.ThreadPoolExecutor(max_workers=max(2, cpu))
        self._group_locks: dict[str, asyncio.Lock] = {g.name: asyncio.Lock() for g in config.groups}
        # Sticky per-group series count from the latest successful run.
        # Used to enforce safety.max_total_series across groups —
        # APScheduler fires each group independently, so a per-tick
        # reset would defeat the cross-group cap. Updated when a group
        # finishes (success path); failures leave the old count in place.
        self._last_series_count: dict[str, int] = {}
        # Per-detector compute time captured during the current run;
        # flushed onto the operational gauge at the end of the run.
        self._detect_durations: dict[str, float] = {}
        # Stratified-detector baseline cache. Keyed by (group, query_id)
        # — the heavy multi-week pull lives here and is reused for
        # ``baseline_refresh_interval`` before the next fetch. The short
        # latest-sample window is pulled every refresh and stitched in
        # before scoring. Entries for removed groups / queries are
        # dropped by ``replace_config`` so a reload that trims the
        # config does not leak stale baselines.
        self._stratified_baseline_cache: dict[tuple[str, str], _StratifiedBaseline] = {}
        # Per (group, query.id template) -> dict mapping
        # ``rendered_id`` -> consecutive runs absent. Used to evict
        # stratified baseline cache entries for ``discover:``-templated
        # queries whose rendered ids stop appearing — without this,
        # workloads that churn through ephemeral identities (pod names,
        # replica IDs that never recycle) grow the cache without bound.
        # Ages out on the same ``safety.discovery.forget_after_runs``
        # threshold the snapshot store uses for signal-missing tracking.
        self._discovery_stratified_misses: dict[tuple[str, str], dict[str, int]] = {}
        # Surface stratified-config issues once at boot — but only on
        # single-replica deployments. In HA mode every follower would
        # otherwise log the same warning, multiplying the noise by the
        # replica count without adding information. The leader picks
        # up the warning instead via ``surface_misconfigurations()``,
        # called from the leader-acquisition callback in main.py.
        if not self._config.highAvailability.enabled:
            self.surface_misconfigurations()

    @property
    def config(self) -> Config:
        return self._config

    # ------------------------------------------------------------------
    # Test-friendly accessors. The underscored attributes are
    # implementation detail; these properties exist so tests can
    # introspect cache + discovery state without pinning to the field
    # names (a rename would otherwise cascade into every test).
    # Production code should not use these — read the snapshot store
    # for live metrics or call ``surface_misconfigurations`` for the
    # config-health check.
    # ------------------------------------------------------------------
    def stratified_cache_keys_for_testing(self) -> set[tuple[str, str]]:
        """Snapshot of currently-cached ``(group, rendered_id)`` keys."""
        return set(self._stratified_baseline_cache.keys())

    def stratified_cache_size_for_testing(self) -> int:
        """Total live stratified baseline cache entries."""
        return len(self._stratified_baseline_cache)

    def discovery_stratified_misses_for_testing(
        self,
    ) -> dict[tuple[str, str], dict[str, int]]:
        """Defensive copy of the discovery-churn miss tracker."""
        return {key: dict(inner) for key, inner in self._discovery_stratified_misses.items()}

    def age_stratified_cache_entry_for_testing(
        self, cache_key: tuple[str, str], delta_seconds: float
    ) -> None:
        """Backdate one cache entry's ``fetched_at`` for staleness tests.

        Pure test ergonomics — avoids tests sleeping for the
        ``baseline_refresh_interval`` to observe re-fetch behaviour.
        Raises KeyError if the entry doesn't exist so the test fails
        loudly rather than silently no-op'ing.
        """
        self._stratified_baseline_cache[cache_key].fetched_at -= delta_seconds

    def replace_config(self, new_config: Config) -> None:
        """Hot-replace the config without disposing the runner.

        Called from the reload path. Drops state for groups that were
        removed; new groups simply appear on the next scheduled run.
        The calibration cache is dropped wholesale because its key
        does not include detector ``params`` — a reload that tweaks a
        detector parameter would otherwise reuse stale per-detector F1
        / confidence / stability numbers for up to one
        ``auto_select_interval``.
        """
        old_names = {g.name for g in self._config.groups}
        new_names = {g.name for g in new_config.groups}
        for removed in old_names - new_names:
            self._store.remove_group(removed)
            self._group_locks.pop(removed, None)
        for added in new_names - old_names:
            self._group_locks[added] = asyncio.Lock()
        self._store.clear_calibrations()
        self._prune_stratified_cache_on_reload(new_config)
        # The discovery-churn miss tracker is keyed by the *template*
        # query id; drop entries for templates that no longer exist so a
        # reload that removes a discover: query frees its tracker state
        # along with its cache.
        new_template_keys: set[tuple[str, str]] = {
            (group.name, query.id)
            for group in new_config.groups
            for query in group.queries
            if query.discover
        }
        for tracker_key in list(self._discovery_stratified_misses):
            if tracker_key not in new_template_keys:
                del self._discovery_stratified_misses[tracker_key]
        self._config = new_config
        # Re-check stratified config after the reload — operators may
        # have introduced a bad ``baseline_refresh_interval`` value
        # since boot, and silent strategy-degradation would be worse
        # than a noisy log line. Same single-replica gating as boot —
        # the leader resurfaces on its next acquisition in HA mode.
        if not self._config.highAvailability.enabled:
            self.surface_misconfigurations()

    async def run_group(self, group_name: str) -> GroupRunResult:
        """Execute one detection pass for ``group_name``.

        On success, the snapshot store is updated with the new samples
        and the group's readiness flag flips to True. On failure (any
        reason), the last good snapshot is preserved and an operational
        counter bumps. Either way the operational metrics for this run
        are refreshed.

        ``safety.on_source_failure`` influences whether a TSDB-level
        failure during one query takes the whole group down for this
        run (``serve_stale``, ``fail_ready``) or merely drops the
        failing query's series from the snapshot (``drop_scores``).
        """
        group = self._group_by_name(group_name)
        if group is None:
            raise KeyError(f"unknown group {group_name!r}")

        policy = self._config.safety.on_source_failure
        async with self._group_locks[group_name]:
            self._store.mark_attempted(group_name)
            run_start = time.monotonic()
            self._detect_durations = {}
            result = GroupRunResult(group=group_name)
            source_failed = False
            try:
                with self._telemetry.span(
                    "promanomaly.group_run",
                    group=group_name,
                ):
                    inner = await self._run_group_inner(group)
                result.samples = inner.samples
                result.series_count = inner.series_count
                result.dropped = inner.dropped
                result.source_failures = inner.source_failures
                result.queries_succeeded = inner.queries_succeeded
                # drop_scores edge case: every query in the group hit a
                # source failure. Treat that as a whole-group source
                # failure — preserve the previous snapshot instead of
                # overwriting it with an empty one, and let the streak
                # bookkeeping treat it like a serve_stale failure.
                if (
                    policy == "drop_scores"
                    and inner.source_failures > 0
                    and inner.queries_succeeded == 0
                ):
                    source_failed = True
                    self._ops.failures_total.labels(
                        group=group_name,
                        detector="",
                        reason=REASON_QUERY_EXCEPTION,
                    ).inc()
                else:
                    result.succeeded = True
            except SourceQueryError:
                # In serve_stale / fail_ready, a SourceQueryError
                # propagates out of _run_group_inner because we want to
                # *not* overwrite the snapshot. drop_scores swallows
                # SourceQueryError before it reaches here unless every
                # query failed (handled above).
                source_failed = True
                # Preserve the cross-failure-mode counter contract:
                # a group-aborting source failure still bumps the
                # generic group-level failure counter under
                # ``reason=query_exception``, alongside the more
                # specific ``anomaly_source_failure_total`` already
                # bumped inside ``_run_group_inner``.
                self._ops.failures_total.labels(
                    group=group_name,
                    detector="",
                    reason=REASON_QUERY_EXCEPTION,
                ).inc()
            except Exception as exc:
                logger.exception(
                    "group_run_failed",
                    group=group_name,
                    error=str(exc),
                )
                self._ops.failures_total.labels(
                    group=group_name,
                    detector="",
                    reason=REASON_QUERY_EXCEPTION,
                ).inc()
            duration = time.monotonic() - run_start
            result.duration_seconds = duration

            # Streak bookkeeping is policy-independent so the gauge is
            # always meaningful for operators ("how unstable has this
            # group been?"). fail_ready additionally uses it to drop
            # readiness once the streak crosses safety.fail_ready_after.
            if source_failed:
                streak = self._store.bump_source_failure_streak(group_name)
                if policy == "fail_ready":
                    threshold = self._config.safety.fail_ready_after
                    if streak >= threshold:
                        self._store.set_group_ready(group_name, False)
                        logger.warning(
                            "fail_ready_tripped",
                            group=group_name,
                            consecutive_failures=streak,
                            threshold=threshold,
                        )
            elif result.succeeded:
                self._store.reset_source_failure_streak(group_name)

            self._update_operational_metrics(result)
            if result.succeeded:
                # Sticky series count drives the cross-group cap on the
                # next pass. Only update on success — failed runs must
                # not silently shrink the budget for other groups.
                self._last_series_count[group_name] = result.series_count
                self._store.write(
                    GroupSnapshot(
                        group=group_name,
                        timestamp=time.time(),
                        samples=result.samples,
                    )
                )
            return result

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _group_by_name(self, name: str) -> GroupConfig | None:
        for g in self._config.groups:
            if g.name == name:
                return g
        return None

    async def _run_group_inner(self, group: GroupConfig) -> _GroupInnerResult:
        out = _GroupInnerResult()

        for query in group.queries:
            if query.discover:
                await self._process_discovered_query(group, query, out)
            else:
                await self._process_query_variant(
                    group=group,
                    query=query,
                    variant=ExpandedQuery(
                        rendered_id=query.id,
                        rendered_promql=query.promql,
                        labels={},
                    ),
                    out=out,
                )

        return out

    async def _process_discovered_query(
        self,
        group: GroupConfig,
        query: QueryConfig,
        out: _GroupInnerResult,
    ) -> None:
        """Probe + expand + score one discover-templated query.

        Probe TSDB failures honour ``safety.on_source_failure`` so a
        transient TSDB blip doesn't silently wipe a group's previously-
        published snapshot under ``serve_stale``. Config-level failures
        (undeclared template variable, illegal rendered id) bump
        ``anomaly_discovery_failures_total`` and skip the query without
        invoking the source-failure policy — those are user errors, not
        infrastructure failures.

        Discovery-tracked series that go missing past ``expect_grace_runs``
        trip ``anomaly_signal_missing`` with the discovered labels — the
        v0.4 dynamic counterpart to the v0.1 ``expect:`` flag. The
        rendered (non-template) id is emitted on the metric so the same
        logical series carries the same ``id`` label value in both
        present and missing states.
        """
        policy = self._config.safety.on_source_failure
        try:
            value_sets = await self._probe_discovery_variables(group, query)
        except SourceQueryError as exc:
            # Treat probe-side TSDB failure exactly like a regular
            # source query failure: bump the counter, then either
            # propagate (serve_stale / fail_ready preserve the previous
            # snapshot) or swallow (drop_scores).
            self._ops.source_failure_total.labels(group=group.name, reason=exc.reason).inc()
            logger.warning(
                "discovery_probe_source_failed",
                group=group.name,
                query=query.id,
                reason=exc.reason,
                detail=exc.message,
                policy=policy,
            )
            if policy == "drop_scores":
                out.source_failures += 1
                return
            raise
        except DiscoveryError as exc:
            # Config-level discovery failure (template references an
            # undeclared variable, etc.) — not a transient infra issue,
            # so it shouldn't invoke serve_stale. Just skip the query.
            logger.error(
                "discovery_aborted",
                group=group.name,
                query=query.id,
                reason=exc.reason,
                detail=exc.message,
            )
            return

        try:
            variants = expand_query(query, value_sets)
        except DiscoveryError as exc:
            self._ops.discovery_failures_total.labels(
                group=group.name,
                query=query.id,
                variable="",
                reason=exc.reason,
            ).inc()
            logger.error(
                "discovery_expansion_failed",
                group=group.name,
                query=query.id,
                reason=exc.reason,
            )
            return

        self._ops.discovery_expansions.labels(group=group.name, query=query.id).set(len(variants))

        # Stratified-cache bookkeeping for discovery: ages out cache
        # entries for rendered ids that stop appearing, so a workload
        # with churning identities (transient pods, replica IDs that
        # never recycle) doesn't grow the cache without bound. Cheap
        # no-op when no detector on the query opts into the
        # sliding-window strategy.
        self._prune_stratified_cache_for_discovery(
            group=group,
            query=query,
            active_rendered_ids={v.rendered_id for v in variants},
        )

        seen_keys: list[tuple[tuple[str, str], ...]] = []
        for variant in variants:
            seen_keys.append(variant.label_key)
            await self._process_query_variant(
                group=group,
                query=query,
                variant=variant,
                out=out,
            )

        # Discovery-based absence: anomaly_signal_missing for series
        # we've seen before but is absent in this run for at least
        # ``expect_grace_runs`` consecutive runs. ``forget_after_runs``
        # bounds the tracker so churn through ephemeral identities
        # (transient pods, replicas) can't grow the dict without end.
        grace = query.expect_grace_runs
        forget_after = self._config.safety.discovery.forget_after_runs
        transitioned = self._store.record_discovery_observation(
            group.name,
            query.id,
            seen_keys,
            grace,
            forget_after_runs=forget_after,
        )
        for _series_key in transitioned:
            self._ops.signal_missing_total.labels(
                group=group.name, reason="missing_grace_exhausted"
            ).inc()
        for series_key in self._store.discovery_known_missing(group.name, query.id, grace):
            # Render the id using the discovered labels so a missing
            # series carries the same ``id`` label value it carried
            # when it was present. Without this, an operator alerting
            # on anomaly_signal_missing{id="cpu_busy_node_a"} would
            # never match because the missing-state metric would use
            # the raw template "cpu_busy_{{ instance }}" instead.
            bindings = dict(series_key)
            rendered_id = render_template(query.id, bindings)
            # Discovered labels travel as ordinary source labels on
            # anomaly_signal_missing — so the operator's privacy filter
            # (exporter.output_labels) applies to them too, even on the
            # "series went missing" path. Without this, a customer_id
            # carried via discover: would leak through the absence
            # signal even when scored metrics correctly strip it.
            filtered_disc = self._filter_source_labels(dict(series_key))
            labels = (
                ("id", rendered_id),
                ("group", group.name),
                *filtered_disc.items(),
            )
            out.samples.append(
                Sample(
                    metric="anomaly_signal_missing",
                    labels=tuple(sorted(labels)),
                    value=1.0,
                )
            )

    def surface_misconfigurations(self) -> None:
        """Log boot / reload / leader-acquisition warnings for risky config.

        Categories surfaced; all keep the runner alive because the safe
        fallback (degraded behaviour) is strictly better than refusing
        to start:

        - ``invalid_value`` — ``baseline_refresh_interval`` resolves to
          a non-positive number; the strategy silently degrades to the
          per-run full-window fetch path.
        - ``below_refresh_interval`` — value is shorter than
          ``server.refresh_interval``, so the cache is always stale
          and the heavy long-history query fires every refresh.
        - ``invalid_lookback`` — ``query_window_seconds`` resolves to
          a non-positive number; the bucket baseline collapses.
        - ``cohort_with_auto_select`` — a Cohort plan in a group with
          ``auto_select: true`` would be silently suppressed because
          calibration is skipped for cohort-aware detectors and the
          winner-picking falls back to a single-series detector.

        Public surface because the HA leader-acquisition callback
        calls it directly: followers stay quiet so a cluster only sees
        one copy of the warning per leader instead of one per replica.
        Detector registry lookup failures are tolerated (logged as
        ``unknown_detector`` only when the runner actually tries to
        use that detector — we don't want to double-log here).
        """

        def _resolve(name: str) -> type[Any]:
            return get(name)

        # Cohort + ``auto_select: true`` warning: the runner skips the
        # calibration cycle for cohort-aware plans so they have no F1
        # to compare against. Combined with auto_select's "winner-only"
        # emission rule, that silently drops the Cohort output. Only
        # the literal ``True`` form trips this; ``explicit_plus_best``
        # emits every detector and is fine.
        for group in self._config.groups:
            if group.auto_select is not True:
                continue
            for query in group.queries:
                for entry in query.detectors:
                    try:
                        cls = _resolve(entry.name)
                    except UnknownDetectorError:
                        continue
                    if is_cohort_aware(cls):
                        logger.warning(
                            "cohort_with_auto_select_true",
                            group=group.name,
                            query=query.id,
                            detector=entry.name,
                            instance=entry.instance,
                            note=(
                                "Cohort-aware detectors do not participate "
                                "in the synthetic-injection calibration "
                                "cycle, so auto_select: true silently "
                                "suppresses their output. Use "
                                "auto_select: false or "
                                "auto_select: 'explicit_plus_best', or "
                                "split Cohort into its own group."
                            ),
                        )

        findings = collect_misconfigurations(
            self._config.groups,
            _resolve,
            self._config.server.refresh_interval_seconds,
        )
        for finding in findings:
            kind = finding.get("kind")
            if kind == "invalid_value":
                logger.warning(
                    "stratified_baseline_refresh_invalid",
                    group=finding["group"],
                    query=finding["query"],
                    detector=finding["detector"],
                    instance=finding["instance"],
                    baseline_refresh_seconds=finding["baseline_refresh_seconds"],
                    note=(
                        "baseline_refresh_interval must be > 0; sliding-window "
                        "strategy disabled for this detector"
                    ),
                )
            elif kind == "below_refresh_interval":
                logger.warning(
                    "stratified_baseline_refresh_below_refresh_interval",
                    group=finding["group"],
                    query=finding["query"],
                    detector=finding["detector"],
                    instance=finding["instance"],
                    baseline_refresh_seconds=finding["baseline_refresh_seconds"],
                    refresh_interval_seconds=finding["refresh_interval_seconds"],
                    note=(
                        "baseline_refresh_interval shorter than "
                        "server.refresh_interval: cache always stale, heavy "
                        "fetch fires every refresh"
                    ),
                )
            elif kind == "invalid_lookback":
                logger.warning(
                    "stratified_lookback_invalid",
                    group=finding["group"],
                    query=finding["query"],
                    detector=finding["detector"],
                    instance=finding["instance"],
                    query_window_seconds=finding["query_window_seconds"],
                    note=(
                        "lookback must be > 0; sliding-window strategy "
                        "disabled and bucket baseline will be empty"
                    ),
                )

    def _prune_stratified_cache_on_reload(self, new_config: Config) -> None:
        """Evict stratified cache entries the new config no longer owns.

        Three eviction conditions, all checked against the *template*
        identity (id + promql before any discover: substitution) so a
        reload's effect on a discover query's many rendered descendants
        is uniform:

        - Template id absent from the new config (group removed or
          query renamed / deleted).
        - Template promql changed under the reload — would otherwise
          stitch the old signal's baseline onto the new signal's short
          fetch.
        - Template still present but no longer has a stratified
          detector — the cache entry is dead weight (``_fetch_stratified``
          will never read it) and just consumes memory until forget-
          after-runs trims it.
        """
        # Build the set of (group, template_query_id) -> template_promql
        # for queries in the new config that still have at least one
        # stratified detector. Anything else makes a cache entry
        # ineligible to live through the reload.
        live_templates: dict[tuple[str, str], str] = {}
        for group in new_config.groups:
            for query in group.queries:
                has_stratified = any(
                    _query_has_stratified_detector(entry) for entry in query.detectors
                )
                if has_stratified:
                    live_templates[(group.name, query.id)] = query.promql

        for cache_key in list(self._stratified_baseline_cache):
            cached = self._stratified_baseline_cache[cache_key]
            template_id = cached.template_query_id or cache_key[1]
            template_promql = cached.template_promql or cached.promql
            live_template_promql = live_templates.get((cache_key[0], template_id))
            if live_template_promql is None or live_template_promql != template_promql:
                del self._stratified_baseline_cache[cache_key]

    def _prune_stratified_cache_for_discovery(
        self,
        *,
        group: GroupConfig,
        query: QueryConfig,
        active_rendered_ids: set[str],
    ) -> None:
        """Age out stratified baseline cache entries for vanished discover ids.

        Mirrors the snapshot store's ``forget_after_runs`` semantics for
        the signal-missing tracker, so both state-bounding paths age out
        on the same threshold. Currently-active rendered ids reset the
        miss counter; absent ones increment it and get evicted from the
        cache (and the tracker) once they cross
        ``safety.discovery.forget_after_runs`` consecutive absent runs.

        Cheap when the query has no stratified detectors — the tracker
        dict stays empty and no cache lookups happen.
        """
        forget_after = self._config.safety.discovery.forget_after_runs
        tracker_key = (group.name, query.id)
        misses = self._discovery_stratified_misses.setdefault(tracker_key, {})

        # Register currently-active rendered ids with miss_count=0
        # (new arrivals enter here; returning ones reset).
        for rid in active_rendered_ids:
            misses[rid] = 0

        # Increment misses for known-but-absent rendered ids; evict
        # entries that cross the threshold from both the miss tracker
        # and the stratified cache itself.
        for rid in list(misses):
            if rid in active_rendered_ids:
                continue
            misses[rid] += 1
            if misses[rid] > forget_after:
                del misses[rid]
                self._stratified_baseline_cache.pop((group.name, rid), None)

    async def _probe_discovery_variables(
        self,
        group: GroupConfig,
        query: QueryConfig,
    ) -> dict[str, list[str]]:
        """Run every probe for a query; harvest discovered values.

        Raises :class:`SourceQueryError` when a probe fails against the
        TSDB so the caller can apply ``safety.on_source_failure``. Other
        failure modes (no safe values, …) emit
        ``anomaly_discovery_failures_total`` and either short-circuit
        via :class:`DiscoveryError` (raised) or carry empty sets through
        (the runner treats that like an ``expect:`` empty result).
        """
        defaults = self._config.defaults

        async def _probe(promql: str) -> QueryResult:
            return await self._source.range_query(
                promql=promql,
                end=time.time(),
                window_seconds=defaults.window_seconds,
                step_seconds=defaults.step_seconds,
                max_series=self._config.safety.max_series_per_query,
            )

        value_sets: dict[str, list[str]] = {}
        for var in query.discover:
            try:
                values, dropped = await probe_variable(var, probe_fn=_probe)
            except DiscoveryError as exc:
                # Probe-side source failures (timeout, server_error, …)
                # arrive here with the original TSDB reason preserved.
                # Re-raise as SourceQueryError so the caller can apply
                # ``safety.on_source_failure`` instead of silently
                # overwriting the snapshot with empty data on a TSDB
                # blip. Tag the discovery-specific counter separately so
                # operators can tell discovery failure from regular
                # query failure on dashboards.
                self._ops.discovery_failures_total.labels(
                    group=group.name,
                    query=query.id,
                    variable=var.variable,
                    reason="probe_query_failed",
                ).inc()
                raise SourceQueryError(exc.reason, exc.message) from exc
            if dropped:
                # Surface unsafe values so the operator can fix the
                # query rather than have us silently filter their
                # production labels. Counter is bounded — one bump per
                # affected variable per run.
                self._ops.discovery_failures_total.labels(
                    group=group.name,
                    query=query.id,
                    variable=var.variable,
                    reason="unsafe_value",
                ).inc()
                logger.warning(
                    "discovery_unsafe_values_dropped",
                    group=group.name,
                    query=query.id,
                    variable=var.variable,
                    examples=dropped[:5],
                    dropped_count=len(dropped),
                )
            if not values:
                self._ops.discovery_failures_total.labels(
                    group=group.name,
                    query=query.id,
                    variable=var.variable,
                    reason="empty_probe",
                ).inc()
                logger.info(
                    "discovery_probe_empty",
                    group=group.name,
                    query=query.id,
                    variable=var.variable,
                )
            value_sets[var.variable] = values
        return value_sets

    async def _process_query_variant(
        self,
        *,
        group: GroupConfig,
        query: QueryConfig,
        variant: ExpandedQuery,
        out: _GroupInnerResult,
    ) -> None:
        """Fetch and score one (possibly rendered) query variant.

        For non-discover queries the variant just mirrors the
        ``QueryConfig`` fields verbatim. For discover queries each
        Cartesian fan-out passes through here, so failure isolation /
        ``expect:`` / cardinality safety apply identically to every
        rendered variant.
        """
        defaults = self._config.defaults
        policy = self._config.safety.on_source_failure
        # ``effective_query`` carries the rendered id/promql and the
        # variant's discovered labels. ``query`` keeps the original
        # config (and its templates) for diagnostics + state-store keys
        # the runner maintains per logical query rather than per
        # expansion.
        effective_query = (
            query.model_copy(update={"id": variant.rendered_id, "promql": variant.rendered_promql})
            if variant.labels
            else query
        )

        try:
            with self._telemetry.span(
                "promanomaly.query",
                group=group.name,
                query=effective_query.id,
            ):
                qresult, plans = await self._fetch_and_plan(
                    group,
                    effective_query,
                    inflight_series_count=out.series_count,
                    template_query=query,
                )
        except SourceQueryError as exc:
            logger.warning(
                "source_query_failed",
                group=group.name,
                query=effective_query.id,
                reason=exc.reason,
                detail=exc.message,
                policy=policy,
            )
            self._ops.source_failure_total.labels(group=group.name, reason=exc.reason).inc()
            if policy == "drop_scores":
                out.source_failures += 1
                return
            raise
        except UnknownDetectorError as exc:
            logger.error(
                "unknown_detector",
                group=group.name,
                query=effective_query.id,
                detector=exc.detector_name,
            )
            self._ops.failures_total.labels(
                group=group.name,
                detector=exc.detector_name,
                reason=REASON_UNKNOWN_DETECTOR,
            ).inc()
            return
        except Exception as exc:
            logger.exception(
                "query_failed",
                group=group.name,
                query=effective_query.id,
                error=str(exc),
            )
            self._ops.failures_total.labels(
                group=group.name,
                detector="",
                reason=REASON_QUERY_EXCEPTION,
            ).inc()
            return

        out.queries_succeeded += 1

        if qresult.truncated:
            out.dropped[REASON_CARDINALITY] = out.dropped.get(REASON_CARDINALITY, 0) + 1
            self._ops.series_dropped_total.labels(group=group.name, reason=REASON_CARDINALITY).inc()

        if effective_query.expect:
            misses = self._store.record_expect_observation(
                group.name, effective_query.id, has_results=bool(qresult.series)
            )
            if misses == effective_query.expect_grace_runs:
                self._ops.signal_missing_total.labels(
                    group=group.name, reason="missing_grace_exhausted"
                ).inc()
            if misses >= effective_query.expect_grace_runs:
                out.samples.append(
                    Sample(
                        metric="anomaly_signal_missing",
                        labels=(("group", group.name), ("id", effective_query.id)),
                        value=1.0,
                    )
                )

        # Cohort detectors need cross-member visibility. Build the
        # per-cohort baseline once here (rather than re-deriving it per
        # member) so every member sees the same numbers. Cheap no-op
        # when no plan in the query opts into ``cohort_aware``.
        cohort_context = self._build_cohort_context(plans, qresult.series)

        for series in qresult.series:
            out.series_count += 1
            produced = await self._score_series(
                group=group,
                query=effective_query,
                plans=plans,
                series=series,
                defaults=defaults,
                cohort_context=cohort_context,
            )
            out.samples.extend(produced)

    async def _fetch_and_plan(
        self,
        group: GroupConfig,
        query: QueryConfig,
        *,
        inflight_series_count: int = 0,
        template_query: QueryConfig | None = None,
    ) -> tuple[QueryResult, list[_DetectorPlan]]:
        defaults = self._config.defaults
        # Validate the detector references up front so we fail loudly
        # before paying for the source query.
        plans: list[_DetectorPlan] = []
        for entry in query.detectors:
            cls = get(entry.name)  # may raise UnknownDetectorError
            params = self._merge_params(cls, entry, defaults)
            plans.append(
                _DetectorPlan(name=entry.name, instance=entry.instance, cls=cls, params=params)
            )

        # Split plans into stratified (sliding-window strategy) and
        # plain rolling. Stratified plans expose
        # ``baseline_refresh_interval_seconds`` AND ``query_window_seconds``;
        # plain rolling plans either omit both or expose only the latter
        # (e.g. v0.2 HourOfDayMAD, which fetches its full lookback every
        # refresh).
        short_window_seconds = defaults.window_seconds
        stratified_meta: list[tuple[float, float]] = []  # (window_seconds, refresh_seconds)
        for plan in plans:
            qw = _detector_window_for(plan)
            refresh = _detector_baseline_refresh_for(plan)
            if qw is not None and refresh is not None:
                stratified_meta.append((qw, refresh))
            elif qw is not None and qw > short_window_seconds:
                # Non-stratified detector that nevertheless wants a
                # longer window than the group default — the v0.2
                # HourOfDayMAD pattern. Per-refresh fetch path.
                short_window_seconds = qw

        end = time.time()
        if stratified_meta:
            baseline_window = max(qw for qw, _ in stratified_meta)
            # Most-aggressive cadence wins when a query mixes detectors
            # with different baseline refresh intervals — fresher data
            # for everyone, at the cost of more TSDB load.
            baseline_refresh = min(refresh for _, refresh in stratified_meta)
            result = await self._fetch_stratified(
                group=group,
                query=query,
                end=end,
                short_window_seconds=short_window_seconds,
                baseline_window_seconds=baseline_window,
                baseline_refresh_seconds=baseline_refresh,
                template_query=template_query or query,
            )
        else:
            result = await self._source.range_query(
                promql=query.promql,
                end=end,
                window_seconds=short_window_seconds,
                step_seconds=defaults.step_seconds,
                max_series=self._config.safety.max_series_per_query,
            )

        # Enforce max_total_series across all groups + queries in the
        # current pass. Each group runs in its own scheduler tick, so
        # "total so far" reads from:
        #   - the latest successful series counts of *other* groups
        #     (sticky in self._last_series_count), plus
        #   - whatever this group has already accumulated in this pass.
        cap = self._config.safety.max_total_series
        if cap:
            other_groups_total = sum(
                count for name, count in self._last_series_count.items() if name != group.name
            )
            running_budget = max(0, cap - other_groups_total - inflight_series_count)
            if len(result.series) > running_budget:
                result = QueryResult(series=result.series[:running_budget], truncated=True)
        return result, plans

    async def _fetch_stratified(
        self,
        *,
        group: GroupConfig,
        query: QueryConfig,
        end: float,
        short_window_seconds: float,
        baseline_window_seconds: float,
        baseline_refresh_seconds: float,
        template_query: QueryConfig | None = None,
    ) -> QueryResult:
        """Sliding-window fetch for queries with a stratified detector.

        Pulls the short window every refresh and the long baseline
        window only when the per-(group, query) cache is stale (or the
        baseline window has grown via a reload). The two results are
        stitched per-series — cached baseline samples before the earliest
        timestamp in the short window, then the fresh short window —
        before being handed back to the scoring path.

        TSDB-failure semantics: a baseline fetch raising
        :class:`SourceQueryError` propagates so the surrounding
        ``safety.on_source_failure`` policy applies; a stale-but-present
        cache is not silently reused. A short-window failure likewise
        propagates. The bookkeeping is done in the runner so detectors
        never see partial state.

        ``template_query`` (when set, i.e. for discover-templated
        variants) carries the pre-substitution id + promql. The cache
        records those so reload-time identity checks compare templates
        rather than rendered ids — without that, every reload would
        wipe every discover-derived cache entry (the template id never
        equals the rendered id).
        """
        defaults = self._config.defaults
        max_series = self._config.safety.max_series_per_query
        cache_key = (group.name, query.id)
        template = template_query or query

        # A SourceQueryError on either fetch propagates so the
        # surrounding ``safety.on_source_failure`` policy applies
        # (serve_stale preserves the previous snapshot, drop_scores
        # skips just this query). We log a structured event with the
        # cache-present flag so operators can distinguish "TSDB blip
        # but we have history we're refusing to silently age" from a
        # cold-start failure. The exception still propagates; the
        # log event is purely additional context. See
        # docs/scaling-stratified.md (Caching & HA Behavior).
        try:
            short_result = await self._source.range_query(
                promql=query.promql,
                end=end,
                window_seconds=short_window_seconds,
                step_seconds=defaults.step_seconds,
                max_series=max_series,
            )
        except SourceQueryError as exc:
            logger.warning(
                "stratified_baseline_fetch_failed",
                group=group.name,
                query=query.id,
                phase="short",
                reason=exc.reason,
                detail=exc.message,
                cache_present=cache_key in self._stratified_baseline_cache,
            )
            raise

        cached = self._stratified_baseline_cache.get(cache_key)
        baseline_stale = (
            cached is None
            or (end - cached.fetched_at) >= baseline_refresh_seconds
            or cached.window_seconds != baseline_window_seconds
            or cached.step_seconds != defaults.step_seconds
            # Defensive: if a caller swapped the config in-place (rather
            # than through replace_config), the cache's recorded promql
            # may no longer match. Reload covers the common path; this
            # guard covers anything that doesn't.
            or cached.promql != query.promql
        )
        if baseline_stale:
            try:
                baseline_result = await self._source.range_query(
                    promql=query.promql,
                    end=end,
                    window_seconds=baseline_window_seconds,
                    step_seconds=defaults.step_seconds,
                    max_series=max_series,
                )
            except SourceQueryError as exc:
                logger.warning(
                    "stratified_baseline_fetch_failed",
                    group=group.name,
                    query=query.id,
                    phase="baseline",
                    reason=exc.reason,
                    detail=exc.message,
                    cache_present=cached is not None,
                )
                raise
            cached = _StratifiedBaseline(
                fetched_at=end,
                promql=query.promql,
                window_seconds=baseline_window_seconds,
                step_seconds=defaults.step_seconds,
                series_by_key={s.label_key: s for s in baseline_result.series},
                template_query_id=template.id,
                template_promql=template.promql,
                truncated=baseline_result.truncated,
            )
            self._stratified_baseline_cache[cache_key] = cached

        assert cached is not None  # mypy: established by either branch above
        stitched: list[QuerySeries] = []
        for series in short_result.series:
            base = cached.series_by_key.get(series.label_key)
            stitched.append(_stitch_series(base, series))

        return QueryResult(
            series=stitched,
            truncated=short_result.truncated or cached.truncated,
        )

    def _merge_params(
        self,
        cls: type[Detector],
        entry: DetectorEntry,
        defaults: DefaultsConfig,
    ) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        merged.update(getattr(cls, "defaults", {}) or {})
        merged.update(entry.params or {})
        # Surface group-level defaults so detectors can read window/step
        # without re-plumbing through the runner.
        merged.setdefault("window", defaults.window)
        merged.setdefault("step", defaults.step)
        return merged

    def _detector_input_window_seconds(
        self, plan: _DetectorPlan, defaults: DefaultsConfig
    ) -> float:
        """Return how many seconds of data this detector wants to see.

        Stratified detectors expose ``query_window_seconds`` to expand
        the fetch; that same value is also the detector's intended
        input window. Plain rolling detectors fall back to the group
        default — so when MAD shares a query with HourOfDayMAD, MAD
        still scores on the last ``defaults.window`` of data instead
        of the 7d the fetch pulled.
        """
        override = getattr(plan.cls, "query_window_seconds", None)
        if callable(override):
            value = override(plan.params)
            if value is not None:
                return float(value)
        return defaults.window_seconds

    def _slice_for_detector(
        self, plan: _DetectorPlan, samples: pd.DataFrame, defaults: DefaultsConfig
    ) -> pd.DataFrame:
        """Trim ``samples`` to the trailing window the detector wants."""
        if samples.empty:
            return samples
        desired = self._detector_input_window_seconds(plan, defaults)
        latest_ts = float(samples["timestamp"].iloc[-1])
        cutoff = latest_ts - desired
        sliced = samples[samples["timestamp"] >= cutoff]
        # Always keep at least the most recent sample so the detector
        # has a "latest point" to score even when the slice would be
        # empty (e.g. clock skew, weirdly-spaced data).
        if sliced.empty:
            return samples.tail(1).reset_index(drop=True)
        return sliced.reset_index(drop=True)

    def _build_cohort_context(
        self,
        plans: list[_DetectorPlan],
        series_list: list[QuerySeries],
    ) -> _CohortContext:
        """Pre-compute cohort baselines for every cohort-aware plan.

        For each cohort-aware plan, partitions ``series_list`` by
        ``cohort_key`` (all labels except the configured ``cohort_label``)
        and computes the cross-member median + MAD for each cohort that
        meets ``min_cohort_size``. Cohorts below the floor produce no
        entry — the Cohort detector observes the absence and emits a
        zero-score row, which the runner forwards as ``anomaly_score=0``.

        Costs are bounded by ``safety.max_series_per_query``; on a 1000-
        series query the inner numpy work is microseconds.
        """
        context = _CohortContext()
        if not series_list:
            return context

        for plan in plans:
            if not is_cohort_aware(plan.cls):
                continue
            cohort_label = str(
                plan.params.get(
                    "cohort_label",
                    getattr(plan.cls, "defaults", {}).get("cohort_label", "instance"),
                )
            )
            try:
                min_size = int(
                    plan.params.get(
                        "min_cohort_size",
                        getattr(plan.cls, "defaults", {}).get("min_cohort_size", 5),
                    )
                )
            except (TypeError, ValueError):
                min_size = 5
            context.label_by_plan[(plan.name, plan.instance)] = cohort_label

            # Partition series by cohort key.
            buckets: dict[tuple[tuple[str, str], ...], list[np.ndarray]] = {}
            for series in series_list:
                if not series.samples.shape[0]:
                    continue
                key = cohort_key_for(series.labels, cohort_label)
                values = series.samples["y"].to_numpy(dtype=float, copy=False)
                buckets.setdefault(key, []).append(values)

            for cohort_key, member_arrays in buckets.items():
                if len(member_arrays) < min_size:
                    continue
                baseline = compute_cohort_baseline(member_arrays)
                if baseline is None:
                    continue
                cohort_median, cohort_mad = baseline
                context.baselines[((plan.name, plan.instance), cohort_key)] = _CohortBaseline(
                    cohort_median=cohort_median,
                    cohort_mad=cohort_mad,
                    size=len(member_arrays),
                )
        return context

    async def _score_series(
        self,
        *,
        group: GroupConfig,
        query: QueryConfig,
        plans: list[_DetectorPlan],
        series: QuerySeries,
        defaults: DefaultsConfig,
        cohort_context: _CohortContext | None = None,
    ) -> list[Sample]:
        thresholds = query.alert_thresholds or defaults.alert_thresholds
        threshold_score = thresholds.score
        min_abs = query.min_abs_delta if query.min_abs_delta is not None else defaults.min_abs_delta
        min_rel = (
            query.min_relative_delta
            if query.min_relative_delta is not None
            else defaults.min_relative_delta
        )

        observed_runs = self._store.observe(group.name, query.id, series.label_key)
        sample_count = len(series.samples)
        warm = sample_count < defaults.min_points
        if warm:
            return self._warmup_samples(
                group=group,
                query=query,
                series=series,
                plans=plans,
                policy=defaults.warmup_policy,
                observed_runs=observed_runs,
            )

        # Calibration cycle: refresh per-detector confidence / stability
        # / F1 when stale. Cheap because the TTL (24h default) means we
        # only pay the synthetic-injection cost a small handful of times
        # per series per day. Errors are swallowed — calibration is a
        # secondary signal and must never take a normal detection run
        # down.
        if self._calibration_due(group, query, series, plans, defaults):
            self._refresh_calibrations(group, query, series, plans, defaults, threshold_score)

        winner = self._select_winner(group, query, series, plans) if group.auto_select else None
        # When ensemble: is configured we must execute every detector
        # (even auto_select losers) so the fuser sees every vote.
        # Without ensemble, the historical optimization stands —
        # ``auto_select: true`` skips losers entirely to save compute.
        # ``explicit_plus_best`` always runs every detector because
        # every detector's score gets emitted by design.
        run_losers = group.ensemble is not None or group.auto_select == "explicit_plus_best"

        produced: list[Sample] = []
        outcomes: list[_DetectorOutcome] = []
        for plan in plans:
            is_winner = winner is not None and (plan.name, plan.instance) == winner
            is_suppressed = group.auto_select is True and winner is not None and not is_winner
            if is_suppressed and not run_losers:
                # Skip the detector entirely — same compute profile as
                # the v0.2 baseline before ensemble fusion landed.
                continue
            emit_samples = not is_suppressed
            # Per-detector threshold override. BOCPD's score is a posterior
            # probability in [0, 1], so the group-level alert_thresholds.score
            # (typically 3.0) makes no sense for it. Detectors opt in by
            # surfacing a ``threshold`` parameter; everyone else falls back
            # to the query/group threshold.
            effective_threshold = _effective_threshold(plan, threshold_score)
            # Cohort-aware plans get their per-cohort baseline stamped
            # into a copy of plan.params before scoring. Lookups missing
            # in ``cohort_context`` (too-small cohorts) leave the params
            # untouched; the Cohort detector observes the absent baseline
            # and emits a zero-score row. Cohort label is captured here so
            # ``_format_detector_samples`` can stamp the opt-in
            # ``cohort_label`` output label.
            effective_plan = plan
            cohort_label_extra: str | None = None
            if cohort_context is not None and is_cohort_aware(plan.cls):
                plan_key = (plan.name, plan.instance)
                cohort_label = cohort_context.label_by_plan.get(plan_key)
                if cohort_label is not None:
                    cohort_key = cohort_key_for(series.labels, cohort_label)
                    baseline = cohort_context.baselines.get((plan_key, cohort_key))
                    cohort_label_extra = cohort_label
                    if baseline is not None:
                        params_copy = dict(plan.params)
                        params_copy[_PARAM_COHORT_MEDIAN] = baseline.cohort_median
                        params_copy[_PARAM_COHORT_MAD] = baseline.cohort_mad
                        params_copy[_PARAM_COHORT_SIZE] = baseline.size
                        effective_plan = _DetectorPlan(
                            name=plan.name,
                            instance=plan.instance,
                            cls=plan.cls,
                            params=params_copy,
                        )
            try:
                score_row = await self._run_detector(effective_plan, series)
            except concurrent.futures.TimeoutError:
                logger.warning(
                    "detector_timeout",
                    group=group.name,
                    query=query.id,
                    detector=plan.name,
                    series=series.labels,
                )
                self._ops.failures_total.labels(
                    group=group.name, detector=plan.name, reason=REASON_TIMEOUT
                ).inc()
                continue
            except Exception as exc:
                logger.exception(
                    "detector_exception",
                    group=group.name,
                    query=query.id,
                    detector=plan.name,
                    error=str(exc),
                )
                self._ops.failures_total.labels(
                    group=group.name,
                    detector=plan.name,
                    reason=REASON_DETECTOR_EXCEPTION,
                ).inc()
                continue

            score, outside, samples = self._format_detector_samples(
                group=group,
                query=query,
                series=series,
                plan=plan,
                score_row=score_row,
                threshold_score=effective_threshold,
                min_abs=min_abs,
                min_rel=min_rel,
                defaults=defaults,
                mark_as_winner=is_winner,
                cohort_label_extra=cohort_label_extra,
            )
            outcomes.append(_DetectorOutcome(plan=plan, score=score, outside=outside))
            if emit_samples:
                produced.extend(samples)

        if group.ensemble is not None and outcomes:
            produced.extend(
                self._format_ensemble_samples(
                    group=group,
                    query=query,
                    series=series,
                    outcomes=outcomes,
                    ensemble=group.ensemble,
                )
            )
        return produced

    async def _run_detector(self, plan: _DetectorPlan, series: QuerySeries) -> pd.Series:
        timeout = self._config.safety.detect_timeout_seconds
        loop = asyncio.get_running_loop()
        detector = _build_detector(plan.cls, plan.params)
        sliced = self._slice_for_detector(plan, series.samples, self._config.defaults)
        with self._telemetry.span(
            "promanomaly.detect",
            detector=plan.name,
            window=int(self._detector_input_window_seconds(plan, self._config.defaults)),
        ):
            future = loop.run_in_executor(
                self._executor,
                _invoke_detector,
                detector,
                sliced,
                plan.params,
            )
            started = time.monotonic()
            try:
                df = await asyncio.wait_for(future, timeout=timeout)
            except TimeoutError as exc:
                future.cancel()
                self._detect_durations[plan.name] = self._detect_durations.get(plan.name, 0.0) + (
                    time.monotonic() - started
                )
                raise concurrent.futures.TimeoutError("detector timeout") from exc
            self._detect_durations[plan.name] = self._detect_durations.get(plan.name, 0.0) + (
                time.monotonic() - started
            )
            if df.empty:
                raise RuntimeError(f"detector {plan.name} returned no rows")
            return df.iloc[-1]

    def _warmup_samples(
        self,
        *,
        group: GroupConfig,
        query: QueryConfig,
        series: QuerySeries,
        plans: list[_DetectorPlan],
        policy: str,
        observed_runs: int,
    ) -> list[Sample]:
        del observed_runs  # currently informational only
        produced: list[Sample] = []
        if policy == "suppress":
            return produced
        for plan in plans:
            labels = self._series_labels(group, query, series, plan)
            if policy == "emit_warming_up":
                produced.append(
                    Sample(
                        metric="anomaly_warming_up",
                        labels=labels,
                        value=1.0,
                    )
                )
                continue
            if policy == "emit_with_flag":
                flagged = (*labels, ("warming_up", "true"))
                produced.append(
                    Sample(
                        metric="anomaly_warming_up",
                        labels=labels,
                        value=1.0,
                    )
                )
                produced.append(Sample(metric="anomaly_score", labels=flagged, value=0.0))
                produced.append(
                    Sample(
                        metric="anomaly_outside_threshold",
                        labels=flagged,
                        value=0.0,
                    )
                )
                continue
            raise ValueError(f"unknown warmup_policy {policy!r}")
        return produced

    def _format_detector_samples(
        self,
        *,
        group: GroupConfig,
        query: QueryConfig,
        series: QuerySeries,
        plan: _DetectorPlan,
        score_row: pd.Series,
        threshold_score: float,
        min_abs: float,
        min_rel: float,
        defaults: DefaultsConfig,
        mark_as_winner: bool = False,
        cohort_label_extra: str | None = None,
    ) -> tuple[float, bool, list[Sample]]:
        """Return ``(score, outside, samples)`` for one detector run.

        Splitting the verdict out separately from the rendered samples
        lets the caller feed ensemble fusion without re-parsing the
        emitted Sample list.
        """
        extras: dict[str, str] = {}
        if mark_as_winner:
            # Opt-in label: only emitted when this detector is the
            # auto-select winner for this series. Opt-in labels are
            # non-breaking per the project's API contract.
            extras["best_detector"] = plan.name
        if cohort_label_extra is not None:
            # Opt-in label emitted by the Cohort detector exclusively
            # (set by the runner when a cohort-aware plan scored this
            # series). Carries the *name* of the source label that
            # varies across cohort members so operators can join the
            # composite output against the source signal easily.
            extras["cohort_label"] = cohort_label_extra
        labels = self._series_labels(group, query, series, plan, extras=extras)
        score = float(score_row.get("score", 0.0))
        baseline = float(score_row.get("baseline", 0.0))
        latest_y = float(series.samples["y"].iloc[-1])

        # Detectors that handle their own threshold semantics (BOCPD's
        # probability score, future change-point detectors) flag
        # ``is_outside=True`` in the score row. The runner ORs that with
        # the statistical threshold check so MAD/Hampel/IQR (which leave
        # is_outside=False) keep their existing behaviour.
        detector_says_outside = bool(score_row.get("is_outside", False))
        outside = self._apply_threshold(
            score=score,
            baseline=baseline,
            latest_y=latest_y,
            threshold_score=threshold_score,
            min_abs=min_abs,
            min_rel=min_rel,
            detector_says_outside=detector_says_outside,
        )
        duration_seconds = self._store.update_duration(
            group=group.name,
            query_id=query.id,
            series_key=series.label_key,
            detector_name=plan.name,
            detector_instance=plan.instance,
            outside=outside,
            now=time.time(),
        )

        samples: list[Sample] = [
            Sample(metric="anomaly_score", labels=labels, value=score),
            Sample(
                metric="anomaly_outside_threshold",
                labels=labels,
                value=1.0 if outside else 0.0,
            ),
        ]
        if defaults.emit_baseline:
            samples.append(Sample(metric="anomaly_baseline", labels=labels, value=baseline))
        if defaults.emit_duration:
            samples.append(
                Sample(
                    metric="anomaly_duration_seconds",
                    labels=labels,
                    value=duration_seconds,
                )
            )

        # Change-point detectors (BOCPD, CUSUM, ...) flag their fires via
        # the optional ``change_point`` column in the score row. Other
        # detectors omit the column entirely so they don't pollute
        # ``anomaly_change_point_total`` with always-zero counters.
        if defaults.emit_change_points and "change_point" in score_row.index:
            fired = bool(score_row["change_point"])
            cp_count = self._store.record_change_point(
                group=group.name,
                query_id=query.id,
                series_key=series.label_key,
                detector_name=plan.name,
                detector_instance=plan.instance,
                fired=fired,
            )
            samples.append(
                Sample(
                    metric="anomaly_change_point_total",
                    labels=labels,
                    value=float(cp_count),
                )
            )

        # Confidence / baseline-stability come from the latest
        # calibration cycle, refreshed at most every
        # ``auto_select_interval``. Emitted only when a calibration
        # exists so we never publish made-up numbers — the gauge
        # appearing in /metrics is itself a signal that calibration
        # has run for this series.
        calibration = self._store.get_calibration(
            group.name, query.id, series.label_key, plan.name, plan.instance
        )
        if calibration is not None:
            samples.append(
                Sample(
                    metric="anomaly_confidence_score",
                    labels=labels,
                    value=calibration.confidence_score,
                )
            )
            if defaults.emit_baseline_stability:
                samples.append(
                    Sample(
                        metric="anomaly_baseline_stability",
                        labels=labels,
                        value=calibration.baseline_stability,
                    )
                )
        return score, outside, samples

    @staticmethod
    def _apply_threshold(
        *,
        score: float,
        baseline: float,
        latest_y: float,
        threshold_score: float,
        min_abs: float,
        min_rel: float,
        detector_says_outside: bool = False,
    ) -> bool:
        """Combine the statistical threshold with the practical floors.

        The raw score is unaffected — only the alerting bit is gated, so
        dashboards still see the statistical signal even when the floor
        suppresses the firing. Both floors must be satisfied when set:
        either floor at zero is treated as "no opinion".

        ``detector_says_outside`` lets a detector that knows its own
        threshold semantics (e.g. BOCPD's probability score) override the
        statistical-score check while still passing through the practical
        floors.
        """
        statistical_outside = detector_says_outside or score >= threshold_score
        delta = abs(latest_y - baseline)
        # Zero baseline (sparse counters that haven't fired yet,
        # zero-clamped gauges) makes "fraction of baseline" undefined.
        # We substitute 1.0 so min_relative_delta degenerates into an
        # absolute-units floor in that case — the most operator-friendly
        # behaviour and the one documented in operations.md.
        baseline_for_rel = abs(baseline) if baseline != 0 else 1.0
        rel_delta = delta / baseline_for_rel if min_rel > 0 else 0.0
        practical_outside = True
        if min_abs > 0:
            practical_outside = practical_outside and delta >= min_abs
        if min_rel > 0:
            practical_outside = practical_outside and rel_delta >= min_rel
        return bool(statistical_outside and practical_outside)

    def _format_ensemble_samples(
        self,
        *,
        group: GroupConfig,
        query: QueryConfig,
        series: QuerySeries,
        outcomes: list[_DetectorOutcome],
        ensemble: EnsembleConfig,
    ) -> list[Sample]:
        """Fuse per-detector outcomes into composite metrics.

        Two fusion methods are supported and only two; the project guide
        is deliberate about not growing this list. ``max`` is a logical
        OR; ``voting`` fires only when ``min_detectors`` agree. The
        composite score is the max raw score across detectors in both
        modes, so the gauge is useful as a "how loud is the loudest
        detector" indicator independent of the firing rule.
        """
        # ``id`` and ``group`` are the only labels on composite metrics —
        # we intentionally drop ``detector`` because the composite spans
        # multiple detectors, and source-side labels would balloon the
        # cardinality at fan-in.
        src = self._filter_source_labels(series.labels)
        result: dict[str, str] = dict(src)
        result["id"] = query.id
        result["group"] = group.name
        labels = tuple(sorted(result.items()))

        composite_score = max((o.score for o in outcomes), default=0.0)
        if ensemble.method == "max":
            fires = any(o.outside for o in outcomes)
        elif ensemble.method == "voting":
            agree = sum(1 for o in outcomes if o.outside)
            fires = agree >= ensemble.min_detectors
        else:
            # Defensive: schema already restricts to {max, voting} so
            # this branch only trips on a future enum addition. Treat
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

    def _series_labels(
        self,
        group: GroupConfig,
        query: QueryConfig,
        series: QuerySeries,
        plan: _DetectorPlan,
        extras: dict[str, str] | None = None,
    ) -> tuple[tuple[str, str], ...]:
        # Output-label filtering: explicit drop wins, then allow restricts
        # to an explicit set. The canonical labels (id, group, detector,
        # detector_instance) are always emitted regardless. ``extras``
        # carries opt-in labels (e.g. ``best_detector`` for auto-select)
        # that bypass the allow/drop filter because they are tool-private
        # and feature-gated.
        src = self._filter_source_labels(series.labels)
        result: dict[str, str] = dict(src)
        result["id"] = query.id
        result["group"] = group.name
        result["detector"] = plan.name
        if plan.instance is not None:
            result["detector_instance"] = plan.instance
        if extras:
            result.update(extras)
        return tuple(sorted(result.items()))

    def _calibration_due(
        self,
        group: GroupConfig,
        query: QueryConfig,
        series: QuerySeries,
        plans: list[_DetectorPlan],
        defaults: DefaultsConfig,
    ) -> bool:
        # Calibration runs whenever auto-select is enabled OR the user
        # asked for the confidence / stability metrics. We honour the
        # latter by checking ``defaults.emit_baseline_stability``;
        # if the gauge is disabled and auto-select is off, we never
        # pay the calibration cost.
        if not group.auto_select and not defaults.emit_baseline_stability:
            return False
        if len(series.samples) < defaults.min_points:
            return False
        ttl = group.auto_select_interval_seconds
        now = time.time()
        for plan in plans:
            # Cohort-aware plans never get a calibration record because
            # the synthetic-injection cycle can't reach across members
            # of the cohort. Without this guard, the runner would think
            # calibration was "due" every single tick and burn CPU on a
            # refresh path that immediately bails out per plan.
            if is_cohort_aware(plan.cls):
                continue
            cal = self._store.get_calibration(
                group.name, query.id, series.label_key, plan.name, plan.instance
            )
            if cal is None or (now - cal.last_calibrated) >= ttl:
                return True
        return False

    def _refresh_calibrations(
        self,
        group: GroupConfig,
        query: QueryConfig,
        series: QuerySeries,
        plans: list[_DetectorPlan],
        defaults: DefaultsConfig,
        threshold_score: float,
    ) -> None:
        now = time.time()
        # Coverage target: window / step samples. With the dev defaults
        # (window=15m, step=15s) this is 60 — exactly the min_points
        # floor — but operators commonly run window=1h / step=15s,
        # giving a 240-sample expected coverage.
        expected = max(
            defaults.min_points,
            int(defaults.window_seconds / max(1.0, defaults.step_seconds)),
        )
        for plan in plans:
            # Cohort-aware detectors are single-series-shaped at the
            # ``fit_score`` boundary but depend on injected cross-member
            # baselines for any meaningful score. The synthetic-injection
            # calibration path has no cohort context to thread through,
            # so calibration here would always score 0 -> F1=0 ->
            # confidence/stability collapse to 0 and the operator-visible
            # gauges would publish actively misleading numbers. Skip
            # calibration entirely for cohort-aware plans; the limitation
            # is documented in docs/detectors.md alongside the same
            # caveat that applies to the stratified detectors.
            if is_cohort_aware(plan.cls):
                continue
            cal = self._store.get_calibration(
                group.name, query.id, series.label_key, plan.name, plan.instance
            )
            ttl = group.auto_select_interval_seconds
            if cal is not None and (now - cal.last_calibrated) < ttl:
                continue
            # Use the same slice the detector will see during scoring,
            # so calibration is comparing like with like — especially
            # important when a stratified detector widened the fetch
            # for the rest of the group.
            sliced = self._slice_for_detector(plan, series.samples, defaults)
            # Calibration must use the same threshold the runner would
            # use when scoring this detector live — otherwise BOCPD
            # (probability score) gets compared against the group's
            # sigma-multiplier threshold (typically 3.0) and its F1
            # collapses to 0. Honour the same per-detector ``threshold``
            # override the live-scoring path reads.
            cal_threshold = _effective_threshold(plan, threshold_score)
            try:
                # Match the kwarg-forwarding instantiation path used by
                # ``_run_detector`` — without this, detectors that consume
                # their tuning knobs in ``__init__`` would silently fail
                # calibration (caught and swallowed below) and never get
                # a confidence / baseline-stability gauge.
                factory = _detector_factory_for(plan)
                new_cal = calibrate_detector(
                    samples=sliced,
                    detector_factory=factory,
                    params=plan.params,
                    threshold=cal_threshold,
                    expected_samples=expected,
                    now=now,
                )
            except Exception:
                logger.exception(
                    "calibration_failed",
                    group=group.name,
                    query=query.id,
                    detector=plan.name,
                )
                continue
            self._store.record_calibration(
                group.name,
                query.id,
                series.label_key,
                plan.name,
                plan.instance,
                new_cal,
            )

    def _select_winner(
        self,
        group: GroupConfig,
        query: QueryConfig,
        series: QuerySeries,
        plans: list[_DetectorPlan],
    ) -> tuple[str, str | None] | None:
        # Winner = detector with highest F1; ties broken by precision,
        # then by config order so the result is deterministic.
        best: tuple[str, str | None] | None = None
        best_score: tuple[float, float, int] | None = None
        for idx, plan in enumerate(plans):
            cal: DetectorCalibration | None = self._store.get_calibration(
                group.name, query.id, series.label_key, plan.name, plan.instance
            )
            if cal is None:
                continue
            # Higher idx loses on ties because we want config order to
            # win; negate so the tuple comparison ranks idx ascending.
            score_key = (cal.f1, cal.precision, -idx)
            if best_score is None or score_key > best_score:
                best_score = score_key
                best = (plan.name, plan.instance)
        return best

    def _filter_source_labels(self, labels: dict[str, str]) -> dict[str, str]:
        cfg = self._config.exporter.output_labels
        out = dict(labels)
        if cfg.allow:
            allow = set(cfg.allow)
            out = {k: v for k, v in out.items() if k in allow}
        if cfg.drop:
            drop = set(cfg.drop)
            out = {k: v for k, v in out.items() if k not in drop}
        return out

    def _update_operational_metrics(self, result: GroupRunResult) -> None:
        if result.succeeded:
            self._ops.last_run_timestamp_seconds.labels(group=result.group).set(time.time())
            self._ops.series_count.labels(group=result.group).set(result.series_count)
            self._ops.group_ready.labels(group=result.group).set(1.0)
        else:
            self._ops.group_ready.labels(group=result.group).set(
                1.0 if self._store.group_ready(result.group) else 0.0
            )
        self._ops.run_duration_seconds.labels(group=result.group).set(result.duration_seconds)
        self._ops.source_failure_streak.labels(group=result.group).set(
            float(self._store.source_failure_streak(result.group))
        )
        for detector_name, elapsed in self._detect_durations.items():
            self._ops.detect_duration_seconds.labels(
                group=result.group, detector=detector_name
            ).set(elapsed)


def _detector_window_for(plan: _DetectorPlan) -> float | None:
    """Thin wrapper around the shared stratified helper, bound to a plan."""
    return query_window_seconds_for(plan.cls, plan.params)


def _detector_baseline_refresh_for(plan: _DetectorPlan) -> float | None:
    """Thin wrapper around the shared stratified helper, bound to a plan."""
    return baseline_refresh_seconds_for(plan.cls, plan.params)


def _query_has_stratified_detector(entry: DetectorEntry) -> bool:
    """Best-effort: does this detector entry opt into the sliding-window strategy?

    Resolves the class via the registry; an unknown name resolves to
    False so a misconfigured detector doesn't keep a stratified cache
    entry alive across reload. The runner's unknown-detector handling
    surfaces the bad reference at run time anyway.
    """
    try:
        cls = get(entry.name)
    except Exception:
        return False
    merged: dict[str, Any] = dict(getattr(cls, "defaults", {}) or {})
    merged.update(entry.params or {})
    return (
        query_window_seconds_for(cls, merged) is not None
        and baseline_refresh_seconds_for(cls, merged) is not None
    )


def _stitch_series(base: QuerySeries | None, fresh: QuerySeries) -> QuerySeries:
    """Combine a cached baseline window with the latest short window.

    Cached baseline samples whose timestamp is at or after the earliest
    timestamp in the fresh window are dropped — the fresh fetch is the
    source of truth for the overlap. New series (no cached baseline)
    fall back to the short window unchanged; the detector then either
    handles the small slice or surfaces ``insufficient_data`` per its
    own contract.
    """
    if base is None or base.samples.empty:
        return fresh
    if fresh.samples.empty:
        return base
    earliest_fresh_ts = float(fresh.samples["timestamp"].iloc[0])
    base_keep = base.samples[base.samples["timestamp"] < earliest_fresh_ts]
    if base_keep.empty:
        combined = fresh.samples.reset_index(drop=True)
    else:
        combined = pd.concat([base_keep, fresh.samples], ignore_index=True)
    return QuerySeries(labels=dict(fresh.labels), samples=combined)


def _effective_threshold(plan: _DetectorPlan, fallback: float) -> float:
    """Resolve the alerting threshold for a detector plan.

    A detector that handles its own threshold semantics (BOCPD's
    probability score, future change-point detectors with non-sigma
    scoring) declares a ``threshold`` value in its ``defaults`` /
    ``param_spec``; the value flows through ``plan.params`` and the
    runner uses it instead of the group-level
    ``alert_thresholds.score``. Both the live-scoring path and the
    calibration cycle resolve through this helper so they agree on the
    cutoff — diverging here is the bug that makes BOCPD's auto-select
    F1 collapse to zero.
    """
    raw = plan.params.get("threshold")
    if raw is None:
        return fallback
    try:
        return float(raw)
    except (TypeError, ValueError):
        return fallback


def _invoke_detector(
    detector: Detector, samples: pd.DataFrame, params: dict[str, Any]
) -> pd.DataFrame:
    """Thread-pool entry point.

    Detector compute is CPU-bound and synchronous; isolating it in a
    helper keeps the asyncio event loop free.
    """
    return detector.fit_score(samples, "", params)


def _detector_factory_for(plan: _DetectorPlan) -> Any:
    """Return a zero-arg factory the calibration cycle can call.

    Calibration treats its ``detector_factory`` as a callable returning
    a fresh detector instance; we close over ``plan.cls`` and
    ``plan.params`` so the calibration path uses the same constructor
    semantics as the live detection path.
    """

    def factory() -> Detector:
        return _build_detector(plan.cls, plan.params)

    return factory


def _build_detector(cls: type[Detector], params: dict[str, Any]) -> Detector:
    """Instantiate ``cls`` and forward any ``params`` the constructor accepts.

    Detectors that take no constructor arguments (the historical
    contract: read params off the ``params`` dict in ``fit_score``)
    continue to work because we only filter for names ``__init__``
    actually accepts. Detectors that declare matching keyword args
    receive them as kwargs — the v0.2 "params forwarded as kwargs to
    the detector's constructor" surface. The full params dict is also
    handed to ``fit_score`` so both styles compose freely.
    """
    try:
        sig = inspect.signature(cls)
    except (TypeError, ValueError):
        return cls()
    accepts_kwargs = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
    if accepts_kwargs:
        return cls(**params)
    matched = {k: v for k, v in params.items() if k in sig.parameters}
    return cls(**matched) if matched else cls()


__all__ = [
    "REASON_CARDINALITY",
    "REASON_DETECTOR_EXCEPTION",
    "REASON_INSUFFICIENT_DATA",
    "REASON_QUERY_EXCEPTION",
    "REASON_TIMEOUT",
    "REASON_UNKNOWN_DETECTOR",
    "GroupRunResult",
    "Runner",
]
