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
import os
import time
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import structlog

from .config import Config, DefaultsConfig, DetectorEntry, GroupConfig, QueryConfig
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
        """
        old_names = {g.name for g in self._config.groups}
        new_names = {g.name for g in new_config.groups}
        for removed in old_names - new_names:
            self._store.remove_group(removed)
            self._group_locks.pop(removed, None)
        for added in new_names - old_names:
            self._group_locks[added] = asyncio.Lock()
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

        end = time.time()
        result = await self._source.range_query(
            promql=query.promql,
            end=end,
            window_seconds=defaults.window_seconds,
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

        produced: list[Sample] = []
        for plan in plans:
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

            produced.extend(
                self._format_detector_samples(
                    group=group,
                    query=query,
                    series=series,
                    plan=plan,
                    score_row=score_row,
                    threshold_score=threshold_score,
                    min_abs=min_abs,
                    min_rel=min_rel,
                    defaults=defaults,
                )
            )
        return produced

    async def _run_detector(self, plan: _DetectorPlan, series: QuerySeries) -> pd.Series:
        timeout = self._config.safety.detect_timeout_seconds
        loop = asyncio.get_running_loop()
        detector = plan.cls()
        future = loop.run_in_executor(
            self._executor,
            _invoke_detector,
            detector,
            series.samples,
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
    ) -> list[Sample]:
        labels = self._series_labels(group, query, series, plan)
        score = float(score_row.get("score", 0.0))
        baseline = float(score_row.get("baseline", 0.0))
        latest_y = float(series.samples["y"].iloc[-1])

        statistical_outside = score >= threshold_score
        # Practical-significance floor: both the abs and rel floors must
        # be satisfied for the alerting bit to fire. The raw score is
        # unaffected so dashboards still surface the statistical signal.
        delta = abs(latest_y - baseline)
        baseline_for_rel = abs(baseline) if baseline != 0 else 1.0
        rel_delta = delta / baseline_for_rel if min_rel > 0 else 0.0
        practical_outside = True
        if min_abs > 0:
            practical_outside = practical_outside and delta >= min_abs
        if min_rel > 0:
            practical_outside = practical_outside and rel_delta >= min_rel

        outside = bool(statistical_outside and practical_outside)
        duration_seconds = self._store.update_duration(
            group=group.name,
            query_id=query.id,
            series_key=series.label_key,
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
        return samples

    def _series_labels(
        self,
        group: GroupConfig,
        query: QueryConfig,
        series: QuerySeries,
        plan: _DetectorPlan,
    ) -> tuple[tuple[str, str], ...]:
        # Output-label filtering: explicit drop wins, then allow restricts
        # to an explicit set. The canonical labels (id, group, detector,
        # detector_instance) are always emitted regardless.
        src = self._filter_source_labels(series.labels)
        result: dict[str, str] = dict(src)
        result["id"] = query.id
        result["group"] = group.name
        result["detector"] = plan.name
        if plan.instance is not None:
            result["detector_instance"] = plan.instance
        return tuple(sorted(result.items()))

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


def _invoke_detector(
    detector: Detector, samples: pd.DataFrame, params: dict[str, Any]
) -> pd.DataFrame:
    """Thread-pool entry point.

    Detector compute is CPU-bound and synchronous; isolating it in a
    helper keeps the asyncio event loop free.
    """
    return detector.fit_score(samples, "", params)


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
