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
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from .calibration import DetectorCalibration


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
        # (group, query_id, series_key, detector_name, detector_instance)
        # -> wall-clock seconds since anomaly_outside_threshold last
        # transitioned 0 -> 1. Absent means clear. Reset on restart.
        #
        # The key MUST include the detector identity: when a series is
        # scored by multiple detectors per run, each one publishes its
        # own anomaly_duration_seconds with its own detector label and
        # its own verdict — so they cannot share a transition timer or
        # a non-firing detector would silently reset a firing detector's
        # counter.
        self._anomaly_start: dict[
            tuple[str, str, tuple[tuple[str, str], ...], str, str | None],
            float,
        ] = {}
        # (group, query_id) -> number of consecutive runs in which the
        # query returned an empty result vector. Used by the
        # ``expect: true`` flag.
        self._expect_misses: dict[tuple[str, str], int] = {}
        # group -> consecutive source-failed runs. Used by the
        # ``fail_ready`` policy to decide when sustained failures should
        # flip the group's readiness off.
        self._source_failure_streak: dict[str, int] = {}
        # (group, query_id, series_key, detector_name, detector_instance)
        # -> DetectorCalibration. Populated by the auto-select /
        # confidence-score code path. Rebuilt from zero on restart —
        # the next calibration cycle catches up within
        # ``auto_select_interval`` of the new boot.
        self._calibrations: dict[
            tuple[str, str, tuple[tuple[str, str], ...], str, str | None],
            DetectorCalibration,
        ] = {}
        # (group, query_id, series_key, detector_name, detector_instance)
        # -> cumulative change-point count emitted as
        # ``anomaly_change_point_total``. Rebuilt from zero on restart,
        # which Prometheus's counter-reset detection handles correctly.
        self._change_point_counts: dict[
            tuple[str, str, tuple[tuple[str, str], ...], str, str | None],
            int,
        ] = {}
        # Discovery-based absence tracking. Two-level map:
        #   (group, query_id) -> series_key -> miss_count
        # The series_key holds only the labels coming from the
        # ``discover:`` block (e.g. {"instance": "node-17"}). Series we
        # have seen at least once on a previous run are present in the
        # inner dict with miss_count=0; a series that fails to appear
        # increments its counter and trips ``anomaly_signal_missing``
        # once ``expect_grace_runs`` is hit.
        self._discovered_series: dict[
            tuple[str, str],
            dict[tuple[tuple[str, str], ...], int],
        ] = {}

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
            for cal_key in list(self._calibrations):
                if cal_key[0] == group:
                    del self._calibrations[cal_key]
            for cp_key in list(self._change_point_counts):
                if cp_key[0] == group:
                    del self._change_point_counts[cp_key]
            for disc_key in list(self._discovered_series):
                if disc_key[0] == group:
                    del self._discovered_series[disc_key]

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
        detector_name: str,
        detector_instance: str | None,
        outside: bool,
        now: float,
    ) -> float:
        """Maintain the 0->1 transition timestamp and return current duration.

        Per-detector keying matters: with multiple detectors per series
        (e.g. MAD + Hampel + IQR), a non-firing detector must not pop
        the start timestamp a firing sibling just set. Each
        ``(detector_name, detector_instance)`` pair holds its own
        timer.
        """
        key = (group, query_id, series_key, detector_name, detector_instance)
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

    # ------------------------------------------------------------------
    # Calibration cache (auto-select + confidence-score support)
    # ------------------------------------------------------------------
    def record_calibration(
        self,
        group: str,
        query_id: str,
        series_key: tuple[tuple[str, str], ...],
        detector_name: str,
        detector_instance: str | None,
        calibration: DetectorCalibration,
    ) -> None:
        key = (group, query_id, series_key, detector_name, detector_instance)
        with self._lock:
            self._calibrations[key] = calibration

    def get_calibration(
        self,
        group: str,
        query_id: str,
        series_key: tuple[tuple[str, str], ...],
        detector_name: str,
        detector_instance: str | None,
    ) -> DetectorCalibration | None:
        key = (group, query_id, series_key, detector_name, detector_instance)
        with self._lock:
            return self._calibrations.get(key)

    # ------------------------------------------------------------------
    # Change-point counter (anomaly_change_point_total)
    # ------------------------------------------------------------------
    def record_change_point(
        self,
        group: str,
        query_id: str,
        series_key: tuple[tuple[str, str], ...],
        detector_name: str,
        detector_instance: str | None,
        fired: bool,
    ) -> int:
        """Update the cumulative change-point counter and return the new total.

        Increments when ``fired`` is True; otherwise leaves the counter
        unchanged. Either way returns the current cumulative count so the
        caller can emit ``anomaly_change_point_total`` on every run (the
        counter must be present every scrape, otherwise Prometheus treats
        absence as a series staleness rather than a steady value).
        """
        key = (group, query_id, series_key, detector_name, detector_instance)
        with self._lock:
            count = self._change_point_counts.get(key, 0)
            if fired:
                count += 1
                self._change_point_counts[key] = count
            return count

    def clear_calibrations(self) -> None:
        """Drop the entire calibration cache.

        Called on config reload because the cache key does not include
        detector ``params``: a reload that flips ``Hampel.t0`` from 3
        to 7 would otherwise reuse the t0=3 calibration for up to
        ``auto_select_interval``. Wiping forces the next run to
        re-calibrate against the new params.
        """
        with self._lock:
            self._calibrations.clear()

    # ------------------------------------------------------------------
    # Discovery-based absence tracking (v0.4)
    #
    # The dynamic counterpart to the v0.1 static ``expect:`` flag.
    # ``record_discovery_observation`` is called per-run with the set of
    # series ids seen in this pass; returns the series that have just
    # passed ``expect_grace_runs`` consecutive misses (the new transitions
    # the runner needs to publish ``anomaly_signal_missing=1`` for).
    # ------------------------------------------------------------------
    def record_discovery_observation(
        self,
        group: str,
        query_id: str,
        seen: Iterable[tuple[tuple[str, str], ...]],
        grace_runs: int,
        *,
        forget_after_runs: int | None = None,
    ) -> list[tuple[tuple[str, str], ...]]:
        """Update the discovery miss counters; return newly-missing series.

        ``seen`` is the set of discovery label tuples observed this run.
        A series that previously existed but is now absent has its miss
        counter incremented; once the counter equals ``grace_runs`` the
        series is reported back so the caller bumps the signal-missing
        counter exactly once on the transition.

        ``forget_after_runs`` bounds long-term state growth — when a
        series has been missing for that many consecutive runs it is
        dropped from the tracker. This prevents the per-(group, query)
        dict from growing forever on workloads with ephemeral
        identities (transient pod names, replicas IDs that never
        recycle). ``None`` disables the bound.
        """
        seen_set = {tuple(s) for s in seen}
        key = (group, query_id)
        with self._lock:
            known = self._discovered_series.setdefault(key, {})
            for series_key in seen_set:
                known[series_key] = 0  # reset miss count (and register new)
            transitioned: list[tuple[tuple[str, str], ...]] = []
            for series_key, miss_count in list(known.items()):
                if series_key in seen_set:
                    continue
                new_count = miss_count + 1
                if forget_after_runs is not None and new_count > forget_after_runs:
                    # Long-gone series: drop it so the tracker stays
                    # bounded. If it ever reappears it counts as a
                    # fresh discovery — not a re-emergence — which is
                    # the right semantics for ephemeral identities.
                    del known[series_key]
                    continue
                known[series_key] = new_count
                if new_count == grace_runs:
                    transitioned.append(series_key)
            return transitioned

    def discovery_known_missing(
        self,
        group: str,
        query_id: str,
        grace_runs: int,
    ) -> list[tuple[tuple[str, str], ...]]:
        """All currently-missing series past the grace window.

        Used by the runner to keep emitting ``anomaly_signal_missing=1``
        on every scrape (Prometheus needs the metric to appear in each
        sample) once a series has tripped the threshold.
        """
        key = (group, query_id)
        with self._lock:
            known = self._discovered_series.get(key, {})
            return [s for s, miss in known.items() if miss >= grace_runs]

    def discovery_known_series(
        self,
        group: str,
        query_id: str,
    ) -> set[tuple[tuple[str, str], ...]]:
        """Inspection helper: every discovered key the store remembers."""
        key = (group, query_id)
        with self._lock:
            known = self._discovered_series.get(key, {})
            return set(known.keys())


class SharedSnapshotCache:
    """Redis-backed snapshot cache for HA mode.

    The leader publishes each successful group snapshot with
    :meth:`publish`; followers periodically pull every published
    snapshot via :meth:`load_all` and feed them into their local
    :class:`SnapshotStore` so ``/metrics`` serves the same data on
    every replica.

    Snapshot values are JSON-encoded via :mod:`promanomaly.serde` so a
    write-capable Redis cannot RCE the readers. Each entry sits behind
    a TTL so a stale leader can never leave indefinitely-old data
    lingering in the cache; the cooldown ≤ ``refresh_interval / 2`` on
    demoted leaders keeps split-brain windows shorter than the TTL so
    followers never serve a poisoned snapshot.
    """

    def __init__(
        self,
        *,
        client: Any,
        ttl_seconds: float,
        key_prefix: str = "promanomaly:snapshot",
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        self._client = client
        self._ttl = int(max(1, ttl_seconds))
        self._prefix = key_prefix

    @property
    def ttl_seconds(self) -> float:
        return float(self._ttl)

    def publish(self, snapshot: GroupSnapshot) -> None:
        """Push ``snapshot`` to Redis under the group's key.

        Best-effort: a transient Redis outage logs upstream and the
        leader keeps serving its own in-memory snapshot until Redis
        comes back. The TTL means a leader that dies before its
        followers next poll will not leave a snapshot in place longer
        than ``snapshot_ttl``.
        """
        # Local import: serde imports from state, so importing at module
        # top would be circular. Indirected through the call site
        # because publish is the only thing that needs it.
        from .serde import encode_snapshot

        try:
            payload = encode_snapshot(snapshot)
        except Exception:
            return
        try:
            self._client.set(self._key_for(snapshot.group), payload, ex=self._ttl)
        except Exception:
            return

    def load_all(self) -> list[GroupSnapshot]:
        """Return every snapshot currently published.

        Used by followers on each poll to refresh their local store.
        Errors return an empty list; callers fall back to whatever they
        already have rather than blanking the surface. Entries that
        fail to decode (corrupt, wrong-version, non-JSON) are skipped
        silently — same semantics as a missing entry.
        """
        from .serde import decode_snapshot

        results: list[GroupSnapshot] = []
        try:
            for key in self._client.scan_iter(match=f"{self._prefix}:*", count=200):
                raw = self._client.get(key)
                if raw is None:
                    continue
                snap = decode_snapshot(raw)
                if snap is not None:
                    results.append(snap)
        except Exception:
            return []
        return results

    def remove(self, group: str) -> None:
        try:
            self._client.delete(self._key_for(group))
        except Exception:
            return

    def _key_for(self, group: str) -> str:
        return f"{self._prefix}:{group}"


class FollowerSyncer:
    """Periodically pull leader snapshots into a local SnapshotStore.

    Followers run this in the background so ``/metrics`` returns the
    same wire output as the leader. Cheap — one Redis SCAN per poll,
    plus one GET per published group.
    """

    def __init__(
        self,
        *,
        store: SnapshotStore,
        shared: SharedSnapshotCache,
    ) -> None:
        self._store = store
        self._shared = shared

    def poll_once(self) -> int:
        """Refresh local snapshots from the shared cache. Returns count."""
        snapshots = self._shared.load_all()
        for snap in snapshots:
            self._store.write(snap)
        return len(snapshots)


def _now() -> float:
    return time.time()


__all__ = [
    "FollowerSyncer",
    "GroupSnapshot",
    "Sample",
    "SharedSnapshotCache",
    "SnapshotStore",
]
