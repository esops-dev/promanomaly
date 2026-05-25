"""In-memory snapshot cache + per-series warm-up counters.

Rebuilt from zero on every restart by design — see the project guide's
"Stateless serving" principle. The cache exists so ``/metrics`` always
returns the most recent successful detector output, even when the
scheduler is between runs or when the TSDB transiently fails (the
``serve_stale`` source-failure policy hands the last good snapshot back
unchanged).
"""

from __future__ import annotations

import threading
from collections.abc import Iterable
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Sample:
    """One value emitted on ``/metrics`` — a metric name, labels, and a value.

    The exporter renders one Prometheus exposition line per Sample. Help
    and TYPE annotations are looked up by metric name in the exporter, not
    carried on each Sample.
    """

    metric: str
    labels: tuple[tuple[str, str], ...]
    value: float

    @property
    def label_dict(self) -> dict[str, str]:
        return dict(self.labels)


@dataclass
class GroupSnapshot:
    """The most recent successful run for a group."""

    group: str
    timestamp: float
    samples: list[Sample] = field(default_factory=list)


class SnapshotStore:
    """Thread-safe store of the most recent snapshot per group.

    The HTTP server reads from here on every scrape; the scheduler writes
    after every successful run. Holding the lock for read-and-rebuild
    keeps the wire output internally consistent.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._snapshots: dict[str, GroupSnapshot] = {}
        self._group_ready: dict[str, bool] = {}
        # (group, query_id, series_key) -> number of consecutive runs where
        # this series has been observed. Used to gate warm-up.
        self._observed_runs: dict[tuple[str, str, tuple[tuple[str, str], ...]], int] = {}
        # (group, query_id, series_key) -> wall-clock seconds since
        # anomaly_outside_threshold last transitioned 0 -> 1. 0 (or absent)
        # means clear. Reset on restart.
        self._anomaly_start: dict[tuple[str, str, tuple[tuple[str, str], ...]], float] = {}
        # (group, query_id) -> number of consecutive runs in which the
        # query returned an empty result vector. Used by the
        # ``expect: true`` flag.
        self._expect_misses: dict[tuple[str, str], int] = {}
        # group -> consecutive source-failed runs. Used by the
        # ``fail_ready`` policy to decide when sustained failures should
        # flip the group's readiness off.
        self._source_failure_streak: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Snapshots
    # ------------------------------------------------------------------
    def write(self, snapshot: GroupSnapshot) -> None:
        with self._lock:
            self._snapshots[snapshot.group] = snapshot
            self._group_ready[snapshot.group] = True

    def get(self, group: str) -> GroupSnapshot | None:
        with self._lock:
            return self._snapshots.get(group)

    def all_snapshots(self) -> list[GroupSnapshot]:
        with self._lock:
            return list(self._snapshots.values())

    def all_samples(self) -> Iterable[Sample]:
        for snap in self.all_snapshots():
            yield from snap.samples

    def remove_group(self, group: str) -> None:
        """Drop snapshot + warm-up state for a group removed by a reload."""
        with self._lock:
            self._snapshots.pop(group, None)
            self._group_ready.pop(group, None)
            for obs_key in list(self._observed_runs):
                if obs_key[0] == group:
                    del self._observed_runs[obs_key]
            for dur_key in list(self._anomaly_start):
                if dur_key[0] == group:
                    del self._anomaly_start[dur_key]
            for miss_key in list(self._expect_misses):
                if miss_key[0] == group:
                    del self._expect_misses[miss_key]
            self._source_failure_streak.pop(group, None)

    # ------------------------------------------------------------------
    # Readiness
    # ------------------------------------------------------------------
    def mark_attempted(self, group: str) -> None:
        with self._lock:
            self._group_ready.setdefault(group, False)

    def group_ready(self, group: str) -> bool:
        with self._lock:
            return self._group_ready.get(group, False)

    def any_group_ready(self) -> bool:
        with self._lock:
            return any(self._group_ready.values())

    def ready_groups(self) -> dict[str, bool]:
        with self._lock:
            return dict(self._group_ready)

    def set_group_ready(self, group: str, ready: bool) -> None:
        """Explicitly drive a group's readiness flag.

        Used by the ``fail_ready`` source-failure policy to flip a group
        out of ready state once the failure streak crosses the threshold,
        without otherwise touching the snapshot.
        """
        with self._lock:
            self._group_ready[group] = ready

    # ------------------------------------------------------------------
    # Source-failure streak (consumed by the ``fail_ready`` policy)
    # ------------------------------------------------------------------
    def bump_source_failure_streak(self, group: str) -> int:
        with self._lock:
            count = self._source_failure_streak.get(group, 0) + 1
            self._source_failure_streak[group] = count
            return count

    def reset_source_failure_streak(self, group: str) -> None:
        with self._lock:
            self._source_failure_streak.pop(group, None)

    def source_failure_streak(self, group: str) -> int:
        with self._lock:
            return self._source_failure_streak.get(group, 0)

    # ------------------------------------------------------------------
    # Warm-up
    # ------------------------------------------------------------------
    def observe(self, group: str, query_id: str, series_key: tuple[tuple[str, str], ...]) -> int:
        """Record one observation of ``series_key``; return new run count."""
        key = (group, query_id, series_key)
        with self._lock:
            count = self._observed_runs.get(key, 0) + 1
            self._observed_runs[key] = count
            return count

    def observed_runs(
        self, group: str, query_id: str, series_key: tuple[tuple[str, str], ...]
    ) -> int:
        with self._lock:
            return self._observed_runs.get((group, query_id, series_key), 0)

    def reset_observation(
        self, group: str, query_id: str, series_key: tuple[tuple[str, str], ...]
    ) -> None:
        with self._lock:
            self._observed_runs.pop((group, query_id, series_key), None)

    # ------------------------------------------------------------------
    # Duration tracking (feeds anomaly_duration_seconds when emit_duration
    # is set; the transition-tracking primitive is already useful today).
    # ------------------------------------------------------------------
    def update_duration(
        self,
        group: str,
        query_id: str,
        series_key: tuple[tuple[str, str], ...],
        outside: bool,
        now: float,
    ) -> float:
        """Maintain the 0->1 transition timestamp and return current duration."""
        key = (group, query_id, series_key)
        with self._lock:
            start = self._anomaly_start.get(key)
            if outside:
                if start is None:
                    self._anomaly_start[key] = now
                    return 0.0
                return max(0.0, now - start)
            self._anomaly_start.pop(key, None)
            return 0.0

    # ------------------------------------------------------------------
    # Expect-flag miss tracking
    # ------------------------------------------------------------------
    def record_expect_observation(self, group: str, query_id: str, has_results: bool) -> int:
        """Update the miss counter for a query with ``expect: true``.

        Returns the new miss count (0 when the query returned results).
        """
        key = (group, query_id)
        with self._lock:
            if has_results:
                self._expect_misses[key] = 0
                return 0
            count = self._expect_misses.get(key, 0) + 1
            self._expect_misses[key] = count
            return count


__all__ = [
    "GroupSnapshot",
    "Sample",
    "SnapshotStore",
]
