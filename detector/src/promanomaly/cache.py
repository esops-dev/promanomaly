"""In-process LRU + TTL cache for PromQL range-query responses.

Two unrelated detection paths frequently issue the same PromQL against the
TSDB within a few seconds: an operator who configures the same expression
under multiple ``id``s for separate detectors, an ensemble that runs
several detectors against the same source query, or the natural overlap
between consecutive detector ticks when ``refresh_interval`` is shorter
than the rolling ``step``. Without coordination the source layer fires
the same query at the TSDB once per caller; with the cache enabled the
second-and-later callers see the previous result instead.

This is the single-replica caching layer described in the v0.2 roadmap
story. The Redis-backed variant for HA mode lives in v0.4 and will plug
into the same call site.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass


@dataclass
class CacheStats:
    """Counters surfaced as Prometheus gauges by the exporter."""

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0


class TTLCache[K, V]:
    """Thread-safe LRU cache with a per-entry TTL.

    Single-replica only — entries live in process memory and are dropped
    on restart, mirroring the rest of the detector's
    stateless-across-restarts contract. The Redis-backed HA variant
    (v0.4) plugs into the same call site by satisfying the same
    interface.
    """

    def __init__(self, *, max_entries: int, ttl_seconds: float) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be >= 1")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        self._max = max_entries
        self._ttl = float(ttl_seconds)
        self._lock = threading.RLock()
        # OrderedDict gives us O(1) LRU bookkeeping via move_to_end.
        self._entries: OrderedDict[K, tuple[float, V]] = OrderedDict()
        self.stats = CacheStats()

    @property
    def ttl_seconds(self) -> float:
        return self._ttl

    @property
    def max_entries(self) -> int:
        return self._max

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def get(self, key: K, now: float) -> V | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self.stats.misses += 1
                return None
            cached_at, value = entry
            if (now - cached_at) > self._ttl:
                # Expired: pop and treat as a miss so the caller refetches.
                del self._entries[key]
                self.stats.expirations += 1
                self.stats.misses += 1
                return None
            self._entries.move_to_end(key)
            self.stats.hits += 1
            return value

    def put(self, key: K, value: V, now: float) -> None:
        with self._lock:
            if key in self._entries:
                self._entries.move_to_end(key)
            self._entries[key] = (now, value)
            while len(self._entries) > self._max:
                # popitem(last=False) drops the least-recently-used entry.
                self._entries.popitem(last=False)
                self.stats.evictions += 1


__all__ = ["CacheStats", "TTLCache"]
