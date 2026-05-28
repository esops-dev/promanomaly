"""Per-(group, query) miss-counter for discover-based absence detection.

When a query expands via a ``discover:`` block, the runner needs to
know when a previously-observed series stops appearing in the latest
result set so it can publish ``anomaly_signal_missing=1`` on the
transition. This tracker holds the consecutive-miss counter per
discovered series and reports the series that have just crossed the
``expect_grace_runs`` threshold.

Rebuilt from zero on restart; ephemeral identities are dropped via
``forget_after_runs`` so the per-(group, query) dict cannot grow
without bound on workloads with churning labels.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable

SeriesKey = tuple[tuple[str, str], ...]


class DiscoveryMissTracker:
    """Thread-safe miss-counter for discover-based absence detection."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # (group, query_id) -> series_key -> consecutive miss count.
        self._series: dict[tuple[str, str], dict[SeriesKey, int]] = {}

    def record_observation(
        self,
        group: str,
        query_id: str,
        seen: Iterable[SeriesKey],
        grace_runs: int,
        *,
        forget_after_runs: int | None = None,
    ) -> list[SeriesKey]:
        """Update miss counters; return series that just crossed ``grace_runs``.

        Series in ``seen`` reset to zero; previously-known series not in
        ``seen`` increment. ``forget_after_runs`` bounds long-term state
        growth: missing series past that many runs are forgotten and
        treated as a fresh discovery if they ever reappear.
        """
        seen_set = {tuple(s) for s in seen}
        key = (group, query_id)
        with self._lock:
            known = self._series.setdefault(key, {})
            for series_key in seen_set:
                known[series_key] = 0
            transitioned: list[SeriesKey] = []
            for series_key, miss_count in list(known.items()):
                if series_key in seen_set:
                    continue
                new_count = miss_count + 1
                if forget_after_runs is not None and new_count > forget_after_runs:
                    del known[series_key]
                    continue
                known[series_key] = new_count
                if new_count == grace_runs:
                    transitioned.append(series_key)
            return transitioned

    def known_missing(
        self,
        group: str,
        query_id: str,
        grace_runs: int,
    ) -> list[SeriesKey]:
        """Currently-missing series past the grace window.

        Used by the runner to keep emitting ``anomaly_signal_missing=1``
        on every scrape — Prometheus needs the metric present every
        sample once a series has tripped the threshold.
        """
        key = (group, query_id)
        with self._lock:
            known = self._series.get(key, {})
            return [s for s, miss in known.items() if miss >= grace_runs]

    def known_series(self, group: str, query_id: str) -> set[SeriesKey]:
        """Every discovered key the tracker remembers (inspection only)."""
        key = (group, query_id)
        with self._lock:
            known = self._series.get(key, {})
            return set(known.keys())

    def drop_group(self, group: str) -> None:
        with self._lock:
            for key in list(self._series):
                if key[0] == group:
                    del self._series[key]
