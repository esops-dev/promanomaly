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
from .exporter import OperationalMetrics
from .source import PromQLSource, QueryResult, QuerySeries, SourceQueryError
from .state import GroupSnapshot, Sample, SnapshotStore

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
    ) -> None:
        self._config = config
        self._source = source
        self._store = store
        self._ops = operational
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

    @property
    def config(self) -> Config:
        return self._config

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
        self._config = new_config

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
        defaults = self._config.defaults
        policy = self._config.safety.on_source_failure
        out = _GroupInnerResult()

        for query in group.queries:
            try:
                qresult, plans = await self._fetch_and_plan(
                    group, query, inflight_series_count=out.series_count
                )
            except SourceQueryError as exc:
                # Source failure semantics:
                #   serve_stale (default) -> raise; the run aborts and
                #     the last good snapshot for the whole group stays
                #     put.
                #   drop_scores -> bump the counter, log, and continue
                #     to the next query. The failed query's series are
                #     simply absent from the new snapshot, so Prometheus
                #     staleness handling flags them within a few
                #     scrapes.
                #   fail_ready -> raise like serve_stale; the readiness
                #     bookkeeping lives in run_group().
                logger.warning(
                    "source_query_failed",
                    group=group.name,
                    query=query.id,
                    reason=exc.reason,
                    detail=exc.message,
                    policy=policy,
                )
                self._ops.source_failure_total.labels(group=group.name, reason=exc.reason).inc()
                if policy == "drop_scores":
                    out.source_failures += 1
                    continue
                raise
            except UnknownDetectorError as exc:
                logger.error(
                    "unknown_detector",
                    group=group.name,
                    query=query.id,
                    detector=exc.detector_name,
                )
                self._ops.failures_total.labels(
                    group=group.name,
                    detector=exc.detector_name,
                    reason=REASON_UNKNOWN_DETECTOR,
                ).inc()
                continue
            except Exception as exc:
                logger.exception(
                    "query_failed",
                    group=group.name,
                    query=query.id,
                    error=str(exc),
                )
                self._ops.failures_total.labels(
                    group=group.name,
                    detector="",
                    reason=REASON_QUERY_EXCEPTION,
                ).inc()
                continue

            # A successful fetch counts as a queries_succeeded even when
            # the result vector is empty: the run produced fresh
            # information about this query (expect: handling needs it,
            # and dashboards rely on "we tried and got nothing" being
            # distinct from "we couldn't ask").
            out.queries_succeeded += 1

            if qresult.truncated:
                out.dropped[REASON_CARDINALITY] = out.dropped.get(REASON_CARDINALITY, 0) + 1
                self._ops.series_dropped_total.labels(
                    group=group.name, reason=REASON_CARDINALITY
                ).inc()

            # expect: handling — a query that returns zero series for
            # expect_grace_runs consecutive runs trips
            # anomaly_signal_missing=1.
            if query.expect:
                misses = self._store.record_expect_observation(
                    group.name, query.id, has_results=bool(qresult.series)
                )
                if misses == 0:
                    pass  # series back; signal_missing implicitly clears
                elif misses == query.expect_grace_runs:
                    self._ops.signal_missing_total.labels(
                        group=group.name, reason="missing_grace_exhausted"
                    ).inc()
                if misses >= query.expect_grace_runs:
                    out.samples.append(
                        Sample(
                            metric="anomaly_signal_missing",
                            labels=(("id", query.id), ("group", group.name)),
                            value=1.0,
                        )
                    )

            for series in qresult.series:
                out.series_count += 1
                produced = await self._score_series(
                    group=group,
                    query=query,
                    plans=plans,
                    series=series,
                    defaults=defaults,
                )
                out.samples.extend(produced)

        return out

    async def _fetch_and_plan(
        self,
        group: GroupConfig,
        query: QueryConfig,
        *,
        inflight_series_count: int = 0,
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

        # Stratified detectors (HourOfDayMAD) need a longer query window
        # than the group default. We take the max across all detectors
        # in the query so MAD + Hampel + HourOfDayMAD can coexist
        # without forcing the group's defaults.window upward.
        window_seconds = defaults.window_seconds
        for plan in plans:
            override = getattr(plan.cls, "query_window_seconds", None)
            if callable(override):
                value = override(plan.params)
                if value is not None and value > window_seconds:
                    window_seconds = float(value)

        end = time.time()
        result = await self._source.range_query(
            promql=query.promql,
            end=end,
            window_seconds=window_seconds,
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

    async def _score_series(
        self,
        *,
        group: GroupConfig,
        query: QueryConfig,
        plans: list[_DetectorPlan],
        series: QuerySeries,
        defaults: DefaultsConfig,
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
            try:
                score_row = await self._run_detector(plan, series)
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
        # cardinality at fan-in. This matches the documented label
        # surface in CLAUDE.md.
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
