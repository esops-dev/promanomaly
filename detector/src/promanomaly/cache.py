"""LRU + TTL cache for PromQL range-query responses.

Two unrelated detection paths frequently issue the same PromQL against the
TSDB within a few seconds: an operator who configures the same expression
under multiple ``id``s for separate detectors, an ensemble that runs
several detectors against the same source query, or the natural overlap
between consecutive detector ticks when ``refresh_interval`` is shorter
than the rolling ``step``. Without coordination the source layer fires
the same query at the TSDB once per caller; with the cache enabled the
second-and-later callers see the previous result instead.

Two backends share the same interface so the source layer doesn't care
which is in use:

- :class:`TTLCache` is the in-process LRU used in single-replica mode.
- :class:`RedisQueryCache` is the v0.4 HA variant — leader and followers
  share a Redis so each (promql, window) bucket only hits the TSDB once
  across the whole cluster. Uses JSON serialisation via :mod:`serde` so
  a write-capable but untrusted Redis can't deserialise to RCE.
"""

from __future__ import annotations

import contextlib
import itertools
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

# Monotonic counter handed out to each cache instance on construction.
# The exporter watches ``cache.version`` to detect reload-driven cache
# swaps without relying on ``id()`` (which Python is free to recycle
# once the old cache is GC'd).
_VERSION_COUNTER = itertools.count(1)


def _next_version() -> int:
    return next(_VERSION_COUNTER)


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

    # In-process: ``get``/``put`` are lock-protected memory ops with no
    # network round-trip, so the source layer calls them inline on the
    # event loop. The Redis backend overrides this to True so the source
    # offloads its blocking socket I/O to a worker thread.
    is_blocking_io: bool = False

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
        self.version: int = _next_version()

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


class RedisQueryCache[K, V]:
    """Redis-backed query cache for HA-mode deployments.

    Mirrors the :class:`TTLCache` surface (``get`` / ``put`` / ``stats`` /
    ``len``) so the source layer can hold a single :class:`Any` reference
    that may resolve to either backend. Values are encoded as JSON via
    :mod:`promanomaly.serde` so deserialisation cannot execute code —
    Redis no longer needs to be on the same trust boundary as the
    detector process.

    The eviction counter is left at 0: Redis handles eviction with its
    own ``maxmemory-policy`` rather than a process-local LRU. Operators
    who want a hard bound configure Redis itself; the cache only owns
    the TTL.
    """

    # ``get``/``put`` do blocking Redis socket I/O, so the source layer
    # must offload them to a worker thread rather than running them on the
    # asyncio event loop (where a slow Redis would stall every group's
    # detection run and the /metrics handler).
    is_blocking_io: bool = True

    # Size lookup is throttled to once per this many seconds. Without
    # the throttle, ``__len__`` issues a SCAN over every cache key on
    # every ``/metrics`` render; with a default 15s scrape interval,
    # that's one SCAN per scrape per replica. Operators get
    # near-real-time data without the cost.
    _SIZE_REFRESH_SECONDS = 30.0

    def __init__(
        self,
        *,
        client: Any,
        ttl_seconds: float,
        key_prefix: str = "promanomaly:query",
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        self._client = client
        self._ttl = int(max(1, ttl_seconds))
        self._prefix = key_prefix
        self._lock = threading.RLock()
        self.stats = CacheStats()
        self.version: int = _next_version()
        # Cached size + the wall-clock the cache was last refreshed.
        # Read under ``_lock`` since /metrics renders concurrently with
        # detector runs in HA mode.
        self._cached_size: int = 0
        self._cached_size_at: float = 0.0

    @property
    def ttl_seconds(self) -> float:
        return float(self._ttl)

    @property
    def max_entries(self) -> int:
        # Redis-managed; the cache itself imposes no upper bound.
        return 0

    def __len__(self) -> int:
        # Throttled SCAN: at most one full-prefix walk every
        # ``_SIZE_REFRESH_SECONDS``. Returns the most recent value in
        # between, so the gauge can lag a scrape but never blocks the
        # /metrics render on a Redis round-trip when entries are dense.
        now = time.monotonic()
        with self._lock:
            if now - self._cached_size_at < self._SIZE_REFRESH_SECONDS:
                return self._cached_size
        try:
            count = sum(1 for _ in self._iter_keys())
        except Exception:
            count = self._cached_size
        with self._lock:
            self._cached_size = count
            self._cached_size_at = now
        return count

    def clear(self) -> None:
        try:
            for key in list(self._iter_keys()):
                self._client.delete(key)
        except Exception:
            # Best-effort. A Redis outage during clear is non-fatal —
            # entries TTL out anyway.
            return

    def get(self, key: K, now: float) -> V | None:
        del now  # TTL is enforced by Redis itself
        redis_key = self._encode_key(key)
        try:
            raw = self._client.get(redis_key)
        except Exception:
            with self._lock:
                self.stats.misses += 1
            return None
        with self._lock:
            if raw is None:
                self.stats.misses += 1
                return None
            self.stats.hits += 1
        from .serde import decode_query_result

        value = decode_query_result(raw)
        if value is None:
            # Corrupted / wrong-version / non-JSON entry. Drop it so
            # the cache stays consistent and treat it as a miss.
            with self._lock:
                self.stats.expirations += 1
            with contextlib.suppress(Exception):
                self._client.delete(redis_key)
            return None
        # ``decode_query_result`` returns a concrete QueryResult; the
        # generic ``QueryResult`` in this class signature is the
        # caller-visible payload type.
        return value  # type: ignore[return-value]

    def put(self, key: K, value: V, now: float) -> None:
        del now
        redis_key = self._encode_key(key)
        from .serde import encode_query_result

        try:
            payload = encode_query_result(value)  # type: ignore[arg-type]
        except Exception:
            # Refusing to cache an un-encodable value is preferable to
            # poisoning Redis with garbage that every reader has to
            # discover and drop.
            return
        try:
            self._client.set(redis_key, payload, ex=self._ttl)
        except Exception:
            return

    def _encode_key(self, key: K) -> str:
        # repr is good enough for the cache-key tuples we pass in — they
        # are flat and contain only primitives. Avoids pulling json into
        # the hot path.
        return f"{self._prefix}:{key!r}"

    def _iter_keys(self) -> Any:
        # SCAN with a small COUNT so we don't block Redis with KEYS.
        return self._client.scan_iter(match=f"{self._prefix}:*", count=200)


__all__ = ["CacheStats", "RedisQueryCache", "TTLCache"]
