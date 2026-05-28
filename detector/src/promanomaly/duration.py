"""Per-series threshold-duration tracker.

Feeds ``anomaly_duration_seconds`` — the wall-clock seconds since
``anomaly_outside_threshold`` last transitioned 0 → 1 for a given
``(group, query_id, series, detector, detector_instance)`` tuple. The
counter is held only in process memory: a restart clips active
durations to zero, which dashboards handle gracefully. This is
consistent with the project's stateless-across-restarts guarantee.

Distinct from Alertmanager's ``for:`` clause — that gates alert
firing but never exposes the elapsed time as a queryable value. The
duration gauge lets operators alert on "outside for > 30 min" with a
single PromQL expression and escalate accordingly.
"""

from __future__ import annotations

import threading

# Detector identity must be part of the key: when a series is scored
# by multiple detectors per run, each publishes its own
# ``anomaly_duration_seconds`` with its own detector label and its own
# verdict, so they cannot share a transition timer. A non-firing
# detector would silently reset a firing sibling's counter otherwise.
DurationKey = tuple[str, str, tuple[tuple[str, str], ...], str, str | None]


class DurationTracker:
    """Maintain 0→1 transition timestamps for outside-threshold runs.

    All public methods are thread-safe. Tracker state is rebuilt from
    zero on restart and on config-driven group removal (see
    :meth:`drop_group`).
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._anomaly_start: dict[DurationKey, float] = {}

    def update(
        self,
        group: str,
        query_id: str,
        series_key: tuple[tuple[str, str], ...],
        detector_name: str,
        detector_instance: str | None,
        outside: bool,
        now: float,
    ) -> float:
        """Record the latest outcome and return the current duration in seconds.

        Returns 0.0 on the run where the transition first crosses from
        clear to outside (so dashboards see a strictly-increasing
        gauge from zero), the elapsed seconds on subsequent outside
        runs, and 0.0 again once the series clears.
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

    def drop_group(self, group: str) -> None:
        """Forget all transitions for a group (config reload removed it)."""
        with self._lock:
            for key in list(self._anomaly_start):
                if key[0] == group:
                    del self._anomaly_start[key]

    def size_for_testing(self) -> int:
        with self._lock:
            return len(self._anomaly_start)


__all__ = ["DurationKey", "DurationTracker"]
