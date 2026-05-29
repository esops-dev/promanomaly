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

import json
import threading
from collections.abc import Iterable
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

SeriesKey = tuple[tuple[str, str], ...]


def _encode_series_key(series_key: SeriesKey) -> str:
    """Serialise a series key to a stable string for a Redis hash field.

    The key is already a sorted tuple of ``(label, value)`` pairs, so the
    JSON encoding is deterministic and round-trips back to the same tuple.
    """
    return json.dumps([list(pair) for pair in series_key], separators=(",", ":"))


def _decode_series_key(field: str) -> SeriesKey | None:
    """Inverse of :func:`_encode_series_key`; ``None`` on a corrupt field."""
    try:
        raw = json.loads(field)
    except (TypeError, ValueError):
        return None
    if not isinstance(raw, list):
        return None
    pairs: list[tuple[str, str]] = []
    for item in raw:
        if not isinstance(item, list) or len(item) != 2:
            return None
        k, v = item
        if not isinstance(k, str) or not isinstance(v, str):
            return None
        pairs.append((k, v))
    return tuple(pairs)


class DiscoveryMissTracker:
    """Thread-safe miss-counter for discover-based absence detection."""

    # In-memory tracker: lookups are instant, so callers run it inline on
    # the event loop. The Redis variant flips this so the runner offloads
    # its blocking socket I/O to a worker thread.
    is_blocking_io: bool = False

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


class RedisDiscoveryMissTracker:
    """Redis-backed miss-counter for discover-based absence detection (HA).

    Drop-in replacement for :class:`DiscoveryMissTracker` whose state lives
    in Redis instead of leader-local memory, so a Lease failover no longer
    resets the per-series miss counts: a newly-elected leader inherits them
    and a series that disappeared right before the handover still trips
    ``anomaly_signal_missing`` on schedule rather than restarting its grace
    window from zero. Resolves the HA caveat documented in
    ``docs/operations.md`` for signal-absence detection.

    Only the elected leader runs the detector pipeline, so there is exactly
    one writer; a plain read-modify-write per ``(group, query_id)`` is safe
    without Lua/WATCH. Each ``(group, query_id)`` is one Redis hash whose
    fields are JSON-encoded series keys and whose values are the
    consecutive-miss counts. A generous TTL is refreshed on every write so
    the state survives a failover gap yet a fully-removed deployment's keys
    eventually expire rather than leaking; while a leader is actively
    writing every refresh the key never expires.

    Best-effort: every Redis op is wrapped so a transient Redis outage
    degrades absence detection (no missing-signal emitted that run) rather
    than crashing the detection run — consistent with the shared snapshot
    cache's philosophy. ``forget_after_runs`` bounds the hash size exactly
    as the in-memory tracker does.
    """

    is_blocking_io: bool = True

    def __init__(
        self,
        *,
        client: Any,
        ttl_seconds: float,
        key_prefix: str = "promanomaly:discovery",
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        self._client = client
        self._ttl = int(max(1, ttl_seconds))
        self._prefix = key_prefix

    def _key_for(self, group: str, query_id: str) -> str:
        # group / query_id are validated Prometheus names (no colons), so
        # ':' is an unambiguous separator and the drop_group SCAN glob is
        # safe.
        return f"{self._prefix}:{group}:{query_id}"

    def _load(self, group: str, query_id: str) -> dict[SeriesKey, int]:
        try:
            raw = self._client.hgetall(self._key_for(group, query_id))
        except Exception as exc:
            logger.warning(
                "discovery_redis_load_failed", group=group, query=query_id, error=str(exc)
            )
            return {}
        known: dict[SeriesKey, int] = {}
        for field, value in (raw or {}).items():
            field_str = field.decode("utf-8") if isinstance(field, bytes) else str(field)
            series_key = _decode_series_key(field_str)
            if series_key is None:
                continue
            try:
                known[series_key] = int(value)
            except (TypeError, ValueError):
                continue
        return known

    def record_observation(
        self,
        group: str,
        query_id: str,
        seen: Iterable[SeriesKey],
        grace_runs: int,
        *,
        forget_after_runs: int | None = None,
    ) -> list[SeriesKey]:
        seen_set = {tuple(s) for s in seen}
        known = self._load(group, query_id)
        new_state: dict[SeriesKey, int] = {}
        transitioned: list[SeriesKey] = []
        for series_key in seen_set:
            new_state[series_key] = 0
        for series_key, miss_count in known.items():
            if series_key in seen_set:
                continue
            new_count = miss_count + 1
            if forget_after_runs is not None and new_count > forget_after_runs:
                continue
            new_state[series_key] = new_count
            if new_count == grace_runs:
                transitioned.append(series_key)
        self._persist(group, query_id, new_state)
        return transitioned

    def _persist(self, group: str, query_id: str, state: dict[SeriesKey, int]) -> None:
        key = self._key_for(group, query_id)
        mapping = {_encode_series_key(k): str(v) for k, v in state.items()}
        try:
            pipe = self._client.pipeline()
            pipe.delete(key)
            if mapping:
                pipe.hset(key, mapping=mapping)
                pipe.expire(key, self._ttl)
            pipe.execute()
        except Exception as exc:
            logger.warning(
                "discovery_redis_persist_failed", group=group, query=query_id, error=str(exc)
            )

    def known_missing(
        self,
        group: str,
        query_id: str,
        grace_runs: int,
    ) -> list[SeriesKey]:
        known = self._load(group, query_id)
        return [s for s, miss in known.items() if miss >= grace_runs]

    def known_series(self, group: str, query_id: str) -> set[SeriesKey]:
        return set(self._load(group, query_id).keys())

    def drop_group(self, group: str) -> None:
        try:
            keys = list(self._client.scan_iter(match=f"{self._prefix}:{group}:*", count=200))
            if keys:
                self._client.delete(*keys)
        except Exception as exc:
            logger.warning("discovery_redis_drop_failed", group=group, error=str(exc))
