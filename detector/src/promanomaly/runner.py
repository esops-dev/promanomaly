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
import contextvars
import os
import time
from dataclasses import dataclass, field
from typing import Any, cast

import pandas as pd
import structlog

from .anomaly_type import classify_window, coerce_declared_type
from .calibration import calibrate_detector
from .cohort_context import CohortContext, build_cohort_context
from .config import (
    Config,
    DefaultsConfig,
    DetectorEntry,
    GroupConfig,
    QueryConfig,
)
from .detectors import Detector, UnknownDetectorError, get
from .detectors.cohort import (
    _PARAM_COHORT_MAD,
    _PARAM_COHORT_MEDIAN,
    _PARAM_COHORT_SIZE,
    cohort_key_for,
    is_cohort_aware,
)
from .discovery import (
    DiscoveryError,
    ExpandedQuery,
    expand_query,
    probe_variable,
    render_template,
)
from .ensemble import DetectorOutcome
from .ensemble import fuse as fuse_ensemble
from .exporter import OperationalMetrics
from .plan import (
    DetectorPlan,
    build_detector,
    build_detector_plans,
    effective_threshold,
    merge_params,
)
from .selector import select_winner
from .self_observability import estimate_samples_bytes
from .severity import compute_severity
from .source import PromQLSource, QueryResult, QuerySeries, SourceQueryError
from .state import GroupSnapshot, Sample, SnapshotStore
from .stratified import (
    baseline_refresh_seconds_for,
    collect_misconfigurations,
    detector_input_window_seconds,
    query_window_seconds_for,
)
from .stratified_fetcher import StratifiedFetcher
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

# Per-dimension cap on the fleet-density slice partition
# (``defaults.density_by``). density_by is meant for low-cardinality
# dimensions; this bounds the rollup cardinality if an operator points it
# at a high-cardinality label by mistake. A run producing more distinct
# slice values than this skips that dimension and logs the drop.
_MAX_DENSITY_SLICES = 200

# Per-detector annotation labels that are NOT part of a series' *entity*
# identity for the fleet-density rollup. ``detector`` / ``detector_instance``
# distinguish detectors scoring the same series; ``cohort_label`` is stamped
# only by cohort-aware detectors (it carries the *name* of the varying label,
# not a per-entity value). Stripping all three makes a series count once per
# (id, group, source-labels) entity no matter how many — or which kind of —
# detectors scored it, so ``anomaly_density`` is a true fraction of entities.
# Without ``cohort_label`` here, a host watched by both a per-host detector
# (MAD) and a Cohort detector counts as two entities and density skews.
_DENSITY_NON_IDENTITY_LABELS = frozenset({"detector", "detector_instance", "cohort_label"})


# Re-exported under the historical private name so the dozens of in-file
# references stay tight. :class:`DetectorPlan` is the public alias.
_DetectorPlan = DetectorPlan


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


# Per-detector compute time captured during a single group run, flushed
# onto the operational gauge at the end of that run. A ContextVar rather
# than an instance attribute because the scheduler fires every group as
# its own concurrent asyncio task against one shared Runner; an instance
# dict would be reset and mutated by all groups at once, cross-attributing
# detector durations to groups that never ran them (e.g. a MAD-only group
# reporting a Hampel duration). Each group run gets a fresh task-local dict
# via ``_DETECT_DURATIONS.set`` so the timings stay scoped to their group.
_DETECT_DURATIONS: contextvars.ContextVar[dict[str, float]] = contextvars.ContextVar(
    "promanomaly_detect_durations"
)


def _current_detect_durations() -> dict[str, float]:
    """Return the current run's duration accumulator, creating one if unset.

    The real pipeline always calls :meth:`Runner.run_group`, which seeds the
    accumulator before any detector runs; the fallback only matters for tests
    that exercise :meth:`Runner._run_detector` in isolation.
    """
    try:
        return _DETECT_DURATIONS.get()
    except LookupError:
        durations: dict[str, float] = {}
        _DETECT_DURATIONS.set(durations)
        return durations


# Per-detector (attempts, successes) accumulator for one group run,
# flushed onto the rolling ``anomaly_detect_success_ratio`` ledger at the
# end of that run. Task-local for the same reason as ``_DETECT_DURATIONS``
# — concurrent group runs share one Runner and an instance dict would
# cross-attribute outcomes between groups. Recorded once per run rather
# than per series so the rolling ledger stays bounded at runs-per-window.
_DETECT_OUTCOMES: contextvars.ContextVar[dict[str, list[int]]] = contextvars.ContextVar(
    "promanomaly_detect_outcomes"
)


def _record_detector_attempt(detector_name: str, *, success: bool) -> None:
    """Tally one detector attempt for the current group run.

    No-op outside a seeded run context (the isolated-``_run_detector``
    tests) so unit tests of a single detector don't need to set up the
    ContextVar. ``[attempts, successes]`` per detector name.
    """
    try:
        outcomes = _DETECT_OUTCOMES.get()
    except LookupError:
        return
    tally = outcomes.setdefault(detector_name, [0, 0])
    tally[0] += 1
    if success:
        tally[1] += 1


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
        # Per-detector compute time is tracked in the ``_DETECT_DURATIONS``
        # ContextVar (task-local per group run), not on the instance — see
        # the module-level note for why a shared dict races across groups.
        # Stratified baseline cache + sliding-window fetch live behind
        # this facade; the runner delegates to it rather than owning
        # the heavy cache state directly.
        self._stratified = StratifiedFetcher(source=source, config=config)
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
        return self._stratified.cache_keys()

    def stratified_cache_size_for_testing(self) -> int:
        """Total live stratified baseline cache entries."""
        return self._stratified.cache_size()

    def discovery_stratified_misses_for_testing(
        self,
    ) -> dict[tuple[str, str], dict[str, int]]:
        """Defensive copy of the discovery-churn miss tracker."""
        return self._stratified.discovery_misses_copy()

    def age_stratified_cache_entry_for_testing(
        self, cache_key: tuple[str, str], delta_seconds: float
    ) -> None:
        """Backdate one cache entry's ``fetched_at`` for staleness tests."""
        self._stratified.age_cache_entry(cache_key, delta_seconds)

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
        # Bound the rolling detector-success ledger to live groups so a
        # group removed by a reload stops accumulating in-memory history
        # (its gauge row lingers until restart, consistent with the other
        # per-group operational gauges).
        self._ops.detect_outcomes.retain_groups(new_names)
        self._store.clear_calibrations()
        # StratifiedFetcher owns its cache + discovery-churn tracker;
        # ``replace_config`` here both updates its config snapshot and
        # prunes the entries the new shape no longer covers.
        self._stratified.replace_config(new_config)
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
            # Seed a fresh, task-local duration accumulator for this run.
            # Isolated per group because each group runs in its own
            # concurrent scheduler task (see ``_DETECT_DURATIONS`` note).
            _DETECT_DURATIONS.set({})
            _DETECT_OUTCOMES.set({})
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

        # Fleet anomaly-density rollup. Bounded-cardinality
        # operational signal computed from this run's own samples — no
        # new per-series cardinality and no extra TSDB queries. Appended
        # to the snapshot so followers serve it and serve_stale keeps the
        # last good value during source outages, consistent with every
        # other snapshot metric.
        out.samples.extend(self._density_samples(group, out.samples))

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
        dynamic counterpart to the static ``expect:`` flag. The
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
        self._stratified.prune_for_discovery(
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
        # In HA mode the tracker is Redis-backed (blocking socket I/O), so
        # offload the read-modify-write off the event loop. Single-replica
        # uses the in-memory tracker and runs inline.
        if self._store.discovery_blocking_io:
            transitioned = await asyncio.to_thread(
                self._store.record_discovery_observation,
                group.name,
                query.id,
                seen_keys,
                grace,
                forget_after_runs=forget_after,
            )
        else:
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
        if self._store.discovery_blocking_io:
            known_missing = await asyncio.to_thread(
                self._store.discovery_known_missing, group.name, query.id, grace
            )
        else:
            known_missing = self._store.discovery_known_missing(group.name, query.id, grace)
        for series_key in known_missing:
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
        self._check_cohort_with_auto_select()
        self._check_stratified_findings()

    def _check_cohort_with_auto_select(self) -> None:
        """Cohort + ``auto_select: true`` -> silently-suppressed output.

        The runner skips the calibration cycle for cohort-aware plans
        so they have no F1 to compare against. Combined with
        ``auto_select``'s "winner-only" emission rule, that silently
        drops the Cohort output. Only literal ``True`` trips this;
        ``explicit_plus_best`` emits every detector and is fine.
        """
        for group in self._config.groups:
            if group.auto_select is not True:
                continue
            for query in group.queries:
                for entry in query.detectors:
                    try:
                        cls = get(entry.name)
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

    def _check_stratified_findings(self) -> None:
        """Dispatch stratified-config findings to their per-kind logger."""
        findings = collect_misconfigurations(
            self._config.groups,
            get,
            self._config.server.refresh_interval_seconds,
        )
        for finding in findings:
            kind = finding.get("kind")
            logger_fn = _MISCONFIG_LOGGERS.get(kind) if isinstance(kind, str) else None
            if logger_fn is not None:
                logger_fn(finding)

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
        # expansion. A lightweight delegating wrapper avoids re-running
        # pydantic validation on every fan-out — at hundreds of variants
        # per refresh ``model_copy`` was a measurable per-tick cost. The
        # ``cast`` here reflects the wrapper's behavioural contract:
        # every attribute the runner reads on a ``QueryConfig`` resolves
        # through ``_QueryVariant.__getattr__``.
        effective_query = cast(
            QueryConfig,
            _QueryVariant(query, variant.rendered_id, variant.rendered_promql)
            if variant.labels
            else query,
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
        cohort_context = build_cohort_context(plans, qresult.series)

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
        plans = build_detector_plans(query, defaults)

        # Split plans into stratified (sliding-window strategy) and
        # plain rolling. Stratified plans expose
        # ``baseline_refresh_interval_seconds`` AND ``query_window_seconds``;
        # plain rolling plans either omit both or expose only the latter
        # (e.g. HourOfDayMAD, which fetches its full lookback every
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
                # longer window than the group default — the
                # HourOfDayMAD pattern. Per-refresh fetch path.
                short_window_seconds = qw

        end = time.time()
        if stratified_meta:
            baseline_window = max(qw for qw, _ in stratified_meta)
            # Most-aggressive cadence wins when a query mixes detectors
            # with different baseline refresh intervals — fresher data
            # for everyone, at the cost of more TSDB load.
            baseline_refresh = min(refresh for _, refresh in stratified_meta)
            result = await self._stratified.fetch(
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

    def _merge_params(
        self,
        cls: type[Detector],
        entry: DetectorEntry,
        defaults: DefaultsConfig,
    ) -> dict[str, Any]:
        return merge_params(cls, entry.params, defaults)

    def _detector_input_window_seconds(
        self, plan: _DetectorPlan, defaults: DefaultsConfig
    ) -> float:
        """Thin wrapper around :func:`detector_input_window_seconds`."""
        return detector_input_window_seconds(plan.cls, plan.params, defaults.window_seconds)

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

    async def _score_series(
        self,
        *,
        group: GroupConfig,
        query: QueryConfig,
        plans: list[_DetectorPlan],
        series: QuerySeries,
        defaults: DefaultsConfig,
        cohort_context: CohortContext | None = None,
    ) -> list[Sample]:
        thresholds = query.alert_thresholds or defaults.alert_thresholds
        threshold_score = thresholds.score
        min_abs = query.min_abs_delta if query.min_abs_delta is not None else defaults.min_abs_delta
        min_rel = (
            query.min_relative_delta
            if query.min_relative_delta is not None
            else defaults.min_relative_delta
        )

        # ``observe()`` is consumed by the ``/debug/inspect`` endpoint
        # (warmup_observed_runs); we don't gate the warmup branch on it
        # because a sparse series with min_points samples in-window
        # is genuinely ready to score regardless of run count.
        self._store.observe(group.name, query.id, series.label_key)
        sample_count = len(series.samples)
        warm = sample_count < defaults.min_points
        # Filter once per series; the same filtered dict is threaded into
        # ``_warmup_samples``, ``_format_detector_samples`` (per plan), and
        # ``fuse_ensemble`` so the allow/drop scan doesn't repeat once per
        # detector. At 10k series x several detectors this was material.
        filtered_labels = self._filter_source_labels(series.labels)
        if warm:
            return self._warmup_samples(
                group=group,
                query=query,
                series=series,
                plans=plans,
                policy=defaults.warmup_policy,
                filtered_labels=filtered_labels,
            )

        # Calibration cycle: refresh per-detector confidence / stability
        # / F1 when stale. Cheap because the TTL (24h default) means we
        # only pay the synthetic-injection cost a small handful of times
        # per series per day. Errors are swallowed — calibration is a
        # secondary signal and must never take a normal detection run
        # down. Run on the shared executor: synthetic-injection plus N
        # detector replays is CPU-bound and would otherwise stall every
        # other coroutine on the event loop for tens of ms per series.
        if self._calibration_due(group, query, series, plans, defaults):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                self._executor,
                self._refresh_calibrations,
                group,
                query,
                series,
                plans,
                defaults,
                threshold_score,
            )

        winner = (
            select_winner(
                group_name=group.name,
                query_id=query.id,
                series_label_key=series.label_key,
                plans=plans,
                get_calibration=self._store.get_calibration,
            )
            if group.auto_select
            else None
        )
        # When ensemble: is configured we must execute every detector
        # (even auto_select losers) so the fuser sees every vote.
        # Without ensemble, the historical optimization stands —
        # ``auto_select: true`` skips losers entirely to save compute.
        # ``explicit_plus_best`` always runs every detector because
        # every detector's score gets emitted by design.
        run_losers = group.ensemble is not None or group.auto_select == "explicit_plus_best"

        produced: list[Sample] = []
        outcomes: list[DetectorOutcome] = []
        for plan in plans:
            is_winner = winner is not None and (plan.name, plan.instance) == winner
            is_suppressed = group.auto_select is True and winner is not None and not is_winner
            if is_suppressed and not run_losers:
                # Skip the detector entirely — same compute profile as
                # the baseline before ensemble fusion landed.
                continue
            emit_samples = not is_suppressed
            # Per-detector threshold override. BOCPD's score is a posterior
            # probability in [0, 1], so the group-level alert_thresholds.score
            # (typically 3.0) makes no sense for it. Detectors opt in by
            # surfacing a ``threshold`` parameter; everyone else falls back
            # to the query/group threshold.
            plan_threshold = effective_threshold(plan.params, threshold_score)
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
                _record_detector_attempt(plan.name, success=False)
                continue
            except Exception as exc:
                logger.exception(
                    "detector_exception",
                    group=group.name,
                    query=query.id,
                    detector=plan.name,
                    exception_type=type(exc).__name__,
                    error=str(exc),
                )
                self._ops.failures_total.labels(
                    group=group.name,
                    detector=plan.name,
                    reason=REASON_DETECTOR_EXCEPTION,
                ).inc()
                _record_detector_attempt(plan.name, success=False)
                continue
            _record_detector_attempt(plan.name, success=True)

            score, outside, samples = self._format_detector_samples(
                group=group,
                query=query,
                series=series,
                plan=plan,
                score_row=score_row,
                threshold_score=plan_threshold,
                min_abs=min_abs,
                min_rel=min_rel,
                defaults=defaults,
                mark_as_winner=is_winner,
                cohort_label_extra=cohort_label_extra,
                filtered_labels=filtered_labels,
            )
            outcomes.append(DetectorOutcome(score=score, outside=outside))
            if emit_samples:
                produced.extend(samples)

        if group.ensemble is not None and outcomes:
            produced.extend(
                fuse_ensemble(
                    group=group,
                    query=query,
                    series=series,
                    source_labels=filtered_labels,
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
            durations = _current_detect_durations()
            try:
                df = await asyncio.wait_for(future, timeout=timeout)
            except TimeoutError as exc:
                future.cancel()
                durations[plan.name] = durations.get(plan.name, 0.0) + (time.monotonic() - started)
                raise concurrent.futures.TimeoutError("detector timeout") from exc
            durations[plan.name] = durations.get(plan.name, 0.0) + (time.monotonic() - started)
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
        filtered_labels: dict[str, str] | None = None,
    ) -> list[Sample]:
        produced: list[Sample] = []
        if policy == "suppress":
            return produced
        for plan in plans:
            labels = self._series_labels(
                group, query, series, plan, filtered_labels=filtered_labels
            )
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
                # Stamp ``warming_up="true"`` on every metric emitted
                # during warmup so the label set stays consistent
                # across the warmup boundary — joins on (id, group)
                # would otherwise break the moment scoring begins.
                flagged = (*labels, ("warming_up", "true"))
                produced.append(
                    Sample(
                        metric="anomaly_warming_up",
                        labels=flagged,
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
        filtered_labels: dict[str, str] | None = None,
    ) -> tuple[float, bool, list[Sample]]:
        """Return ``(score, outside, samples)`` for one detector run.

        Splitting the verdict out separately from the rendered samples
        lets the caller feed ensemble fusion without re-parsing the
        emitted Sample list.
        """
        extras: dict[str, str] = {}
        # The auto-select winner is NOT marked by adding a ``best_detector``
        # label to this detector's samples: the winner can flip every
        # ``auto_select_interval``, and a label that appears/disappears
        # churns the series identity of anomaly_score / outside_threshold /
        # the change-point counter — breaking rate()/increase() on the
        # counter and resetting ``for:`` timers on the outside_threshold
        # alert. Instead the winner is published as a standalone
        # ``anomaly_best_detector{...} = 1`` info gauge below, so the
        # existing per-detector series keep a stable label set.
        if cohort_label_extra is not None:
            # Opt-in label emitted by the Cohort detector exclusively
            # (set by the runner when a cohort-aware plan scored this
            # series). Carries the *name* of the source label that
            # varies across cohort members so operators can join the
            # composite output against the source signal easily.
            extras["cohort_label"] = cohort_label_extra
        labels = self._series_labels(
            group, query, series, plan, extras=extras, filtered_labels=filtered_labels
        )
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
        if mark_as_winner:
            # Standalone info gauge naming the auto-select winner. Shares
            # the per-detector label set (id, group, detector,
            # detector_instance, source labels) so a PromQL join filters
            # the other metrics down to the winner, while keeping the
            # winner marker off those metrics' own label sets (see the
            # note where ``extras`` is built).
            samples.append(Sample(metric="anomaly_best_detector", labels=labels, value=1.0))
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

        # A change-point fire is needed both for the counter below and
        # for anomaly typing; read it once here so the two stay
        # consistent even when emit_change_points is disabled.
        change_point_fired = bool(score_row.get("change_point", False))

        # Change-point detectors (BOCPD, CUSUM, ...) flag their fires via
        # the optional ``change_point`` column in the score row. Other
        # detectors omit the column entirely so they don't pollute
        # ``anomaly_change_point_total`` with always-zero counters.
        if defaults.emit_change_points and "change_point" in score_row.index:
            fired = change_point_fired
            cp_count = self._store.record_change_point(
                group=group.name,
                query_id=query.id,
                series_key=series.label_key,
                detector_name=plan.name,
                detector_instance=plan.instance,
                fired=fired,
            )
            # ``labels`` is now a stable set for this (id, group, detector)
            # series — the auto-select winner marker lives on the separate
            # ``anomaly_best_detector`` gauge rather than as a label here,
            # so this monotonic counter no longer splits across winner /
            # non-winner variants and rate()/increase() stay continuous.
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

        # Operator-facing severity. One normalised 0-1 number that
        # ranks "how much should I care" across detectors whose raw score
        # units differ. Confidence / stability fold in when calibration
        # has run; before that they default to neutral so a strongly
        # firing un-calibrated series still ranks high. Severity is 0 when
        # the firing was gated out (sub-threshold or practical floor).
        if defaults.emit_severity:
            severity = compute_severity(
                score=score,
                threshold=threshold_score,
                detector_name=plan.name,
                outside=outside,
                confidence=calibration.confidence_score if calibration is not None else None,
                baseline_stability=(
                    calibration.baseline_stability if calibration is not None else None
                ),
                duration_seconds=duration_seconds,
            )
            samples.append(Sample(metric="anomaly_severity", labels=labels, value=severity))

        # Anomaly typing (opt-in). A brand-new metric with a frozen
        # ``type`` value-set, emitted only for firing series so the type
        # dimension doesn't multiply non-firing cardinality. Detectors may
        # declare their own ``anomaly_type`` in the score row; otherwise
        # the window heuristic classifies the firing.
        if defaults.emit_anomaly_type and outside:
            declared = coerce_declared_type(score_row.get("anomaly_type"))
            anomaly_kind = declared or classify_window(
                detector_name=plan.name,
                values=series.samples["y"].to_numpy(dtype=float, copy=False),
                baseline=baseline,
                latest_y=latest_y,
                change_point_fired=change_point_fired,
                cohort_aware=is_cohort_aware(plan.cls),
            )
            type_labels = self._series_labels(
                group,
                query,
                series,
                plan,
                extras={**extras, "type": anomaly_kind},
                filtered_labels=filtered_labels,
            )
            samples.append(Sample(metric="anomaly_type_score", labels=type_labels, value=score))
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

    def _series_labels(
        self,
        group: GroupConfig,
        query: QueryConfig,
        series: QuerySeries,
        plan: _DetectorPlan,
        extras: dict[str, str] | None = None,
        *,
        filtered_labels: dict[str, str] | None = None,
    ) -> tuple[tuple[str, str], ...]:
        # Output-label filtering: explicit drop wins, then allow restricts
        # to an explicit set. The canonical labels (id, group, detector,
        # detector_instance) are always emitted regardless. ``extras``
        # carries opt-in labels (e.g. ``best_detector`` for auto-select)
        # that bypass the allow/drop filter because they are tool-private
        # and feature-gated.
        #
        # ``filtered_labels`` lets callers pass an already-filtered copy
        # of ``series.labels`` so a per-series fan-out doesn't re-filter
        # once per detector plan — the runner threads it through from
        # ``_score_series`` to amortise the allow/drop work.
        src = (
            filtered_labels
            if filtered_labels is not None
            else self._filter_source_labels(series.labels)
        )
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
            cal_threshold = effective_threshold(plan.params, threshold_score)
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

    def _filter_source_labels(self, labels: dict[str, str]) -> dict[str, str]:
        cfg = self._config.exporter.output_labels
        # ``allow_set`` / ``drop_set`` are :func:`functools.cached_property`
        # on the model — the frozensets are built once per config load
        # rather than once per filter call (10k series x 3 calls/series
        # was material at scale).
        if not cfg.allow and not cfg.drop:
            return dict(labels)
        if cfg.allow:
            allow = cfg.allow_set
            out = {k: v for k, v in labels.items() if k in allow}
        else:
            out = dict(labels)
        if cfg.drop:
            drop = cfg.drop_set
            out = {k: v for k, v in out.items() if k not in drop}
        return out

    def _density_samples(self, group: GroupConfig, samples: list[Sample]) -> list[Sample]:
        """Build the fleet anomaly-density rollup from a run's own samples.

        A *series* for density purposes is one ``(id, group, source-labels)``
        identity — the per-detector annotation labels
        (``_DENSITY_NON_IDENTITY_LABELS``: ``detector`` /
        ``detector_instance`` / ``cohort_label``) are stripped so a series
        counts once no matter how many — or which kind of — detectors
        scored it. A series is *active* when any detector fired
        (``anomaly_outside_threshold == 1``). Per-series severity is the
        max across that series' detectors.

        Emits (all bounded-cardinality):

        - ``anomaly_active_series{group}`` and ``anomaly_density{group}``
          — the group rollup, always present so panels/alerts never see
          NoData.
        - ``anomaly_density{group, detector}`` — per-detector firing
          fraction.
        - ``anomaly_severity_density{group}`` — severity-weighted density
          (only when severity is being emitted).
        - ``anomaly_density{group, by, <label>}`` (and the severity
          variant) — per-slice partitions for each ``defaults.density_by``
          label, computed from this snapshot only.
        """
        defaults = self._config.defaults
        emit_severity = defaults.emit_severity

        def _series_key(label_dict: dict[str, str]) -> tuple[tuple[str, str], ...]:
            return tuple(
                sorted(
                    (k, v) for k, v in label_dict.items() if k not in _DENSITY_NON_IDENTITY_LABELS
                )
            )

        series_active: dict[tuple[tuple[str, str], ...], bool] = {}
        series_severity: dict[tuple[tuple[str, str], ...], float] = {}
        series_label_dict: dict[tuple[tuple[str, str], ...], dict[str, str]] = {}
        det_total: dict[str, int] = {}
        det_active: dict[str, int] = {}

        for s in samples:
            if s.metric == "anomaly_outside_threshold":
                ld = dict(s.labels)
                # Warming-up series carry a provisional score under the
                # emit_with_flag policy; they have no settled verdict yet,
                # so they are not part of "is the fleet healthy right now".
                if ld.get("warming_up") == "true":
                    continue
                key = _series_key(ld)
                active = s.value >= 1.0
                series_active[key] = series_active.get(key, False) or active
                series_label_dict.setdefault(key, ld)
                detector = ld.get("detector", "")
                det_total[detector] = det_total.get(detector, 0) + 1
                if active:
                    det_active[detector] = det_active.get(detector, 0) + 1
            elif emit_severity and s.metric == "anomaly_severity":
                key = _series_key(dict(s.labels))
                series_severity[key] = max(series_severity.get(key, 0.0), s.value)

        total = len(series_active)
        if total == 0:
            # No scored series this run (a fully warming-up or suppressed
            # group). There is no fleet to measure yet, so emit nothing
            # rather than a misleading 0/0 — the rollup appears as soon as
            # any series produces a verdict.
            return []
        active_count = sum(1 for fired in series_active.values() if fired)
        group_labels: tuple[tuple[str, str], ...] = (("group", group.name),)
        out: list[Sample] = [
            Sample(metric="anomaly_active_series", labels=group_labels, value=float(active_count)),
            Sample(
                metric="anomaly_density",
                labels=group_labels,
                value=(active_count / total) if total else 0.0,
            ),
        ]
        if emit_severity:
            severity_sum = sum(series_severity.get(key, 0.0) for key in series_active)
            out.append(
                Sample(
                    metric="anomaly_severity_density",
                    labels=group_labels,
                    value=(severity_sum / total) if total else 0.0,
                )
            )

        # Per-detector partition.
        for detector in sorted(det_total):
            det_labels = tuple(sorted({"group": group.name, "detector": detector}.items()))
            out.append(
                Sample(
                    metric="anomaly_density",
                    labels=det_labels,
                    value=det_active.get(detector, 0) / det_total[detector],
                )
            )

        # Per-slice partitions (defaults.density_by). One bucket per
        # distinct value of the partition label seen this run.
        for dimension in defaults.density_by:
            buckets_total: dict[str, int] = {}
            buckets_active: dict[str, int] = {}
            buckets_severity: dict[str, float] = {}
            for key, fired in series_active.items():
                value = series_label_dict[key].get(dimension)
                if value is None:
                    continue
                buckets_total[value] = buckets_total.get(value, 0) + 1
                if fired:
                    buckets_active[value] = buckets_active.get(value, 0) + 1
                buckets_severity[value] = buckets_severity.get(value, 0.0) + series_severity.get(
                    key, 0.0
                )
            # Bounded-cardinality guard: density_by is meant for
            # low-cardinality dimensions (namespace, job, region). If an
            # operator points it at a high-cardinality label (pod,
            # instance) the slice partition would explode the rollup
            # cardinality the rest of this metric family is careful to
            # avoid. Cap the emitted slices and log the drop rather than
            # silently truncating or blowing up the TSDB — per the
            # project guide's "no silent caps" rule.
            if len(buckets_total) > _MAX_DENSITY_SLICES:
                logger.warning(
                    "density_by_cardinality_capped",
                    group=group.name,
                    dimension=dimension,
                    distinct_values=len(buckets_total),
                    cap=_MAX_DENSITY_SLICES,
                    note=(
                        "density_by dimension exceeds the per-dimension slice "
                        "cap; partition skipped this run. Point density_by at a "
                        "low-cardinality label (namespace, job, region)."
                    ),
                )
                continue
            for value in sorted(buckets_total):
                slice_labels = tuple(
                    sorted({"group": group.name, "by": dimension, dimension: value}.items())
                )
                out.append(
                    Sample(
                        metric="anomaly_density",
                        labels=slice_labels,
                        value=buckets_active.get(value, 0) / buckets_total[value],
                    )
                )
                if emit_severity:
                    out.append(
                        Sample(
                            metric="anomaly_severity_density",
                            labels=slice_labels,
                            value=buckets_severity[value] / buckets_total[value],
                        )
                    )
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
        # Read the task-local accumulator seeded at the top of this run;
        # ``_update_operational_metrics`` runs in the same task as
        # ``run_group`` so the ContextVar resolves to this group's dict.
        durations = _DETECT_DURATIONS.get({})
        for detector_name, elapsed in durations.items():
            self._ops.detect_duration_seconds.labels(
                group=result.group, detector=detector_name
            ).set(elapsed)
        # Self-observability: attribute this run's detector compute
        # time to the group's cumulative CPU counter, record the
        # per-detector success ledger, and snapshot the group's output
        # memory footprint. CPU is summed across detectors so the counter
        # is a true per-group total regardless of detector count.
        cpu_seconds = sum(durations.values())
        if cpu_seconds > 0:
            self._ops.group_cpu_seconds_total.labels(group=result.group).inc(cpu_seconds)
        for detector_name, (attempts, successes) in _DETECT_OUTCOMES.get({}).items():
            self._ops.detect_outcomes.record(
                group=result.group,
                detector=detector_name,
                attempts=attempts,
                successes=successes,
            )
        if result.succeeded:
            self._ops.group_memory_bytes.labels(group=result.group).set(
                estimate_samples_bytes(result.samples)
            )


# Per-kind log emitters for stratified misconfigurations. Each
# finding dict from :func:`collect_misconfigurations` is dispatched
# through this map by ``kind`` so adding a fifth category is a single-
# line addition rather than another ``elif`` branch.
def _log_invalid_baseline_refresh(finding: dict[str, Any]) -> None:
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


def _log_below_refresh_interval(finding: dict[str, Any]) -> None:
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


def _log_invalid_lookback(finding: dict[str, Any]) -> None:
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


_MISCONFIG_LOGGERS: dict[str, Any] = {
    "invalid_value": _log_invalid_baseline_refresh,
    "below_refresh_interval": _log_below_refresh_interval,
    "invalid_lookback": _log_invalid_lookback,
}


def _detector_window_for(plan: _DetectorPlan) -> float | None:
    """Thin wrapper around the shared stratified helper, bound to a plan."""
    return query_window_seconds_for(plan.cls, plan.params)


def _detector_baseline_refresh_for(plan: _DetectorPlan) -> float | None:
    """Thin wrapper around the shared stratified helper, bound to a plan."""
    return baseline_refresh_seconds_for(plan.cls, plan.params)


# Re-exported under the historical private name so the cli package's
# __all__ and any test monkeypatches keep working without churn.
_build_detector = build_detector


class _QueryVariant:
    """Delegating wrapper used for discover-expanded query variants.

    Overrides ``id`` and ``promql`` (the only fields the runner derives
    per-expansion) and delegates every other attribute read back to the
    underlying :class:`QueryConfig`. The pydantic alternative,
    ``query.model_copy(update={...})``, re-validates the entire model
    on every variant — at hundreds of fan-outs per refresh that was
    the dominant per-tick allocation cost.

    The ``_wrapped_query`` slot is deliberately namespaced out of any
    plausible :class:`QueryConfig` field so a future schema addition
    cannot collide with the wrapper's own state and silently break
    the delegation.
    """

    __slots__ = ("_wrapped_query", "id", "promql")

    def __init__(self, query: QueryConfig, rendered_id: str, rendered_promql: str) -> None:
        self._wrapped_query = query
        self.id = rendered_id
        self.promql = rendered_promql

    def __getattr__(self, name: str) -> Any:
        # ``__getattr__`` only fires when normal lookup fails; ``id``,
        # ``promql``, and ``_wrapped_query`` resolve from the slots above.
        # Everything else is delegated to the underlying QueryConfig.
        return getattr(self._wrapped_query, name)


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
        return build_detector(plan.cls, plan.params)

    return factory


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
