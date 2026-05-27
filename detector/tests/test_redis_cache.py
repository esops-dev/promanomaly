"""Tests for the Redis-backed query cache and shared snapshot cache.

The cache backends are exercised against ``fakeredis`` so the test
suite doesn't require an external Redis. Both backends share the same
shape as the in-process LRU (``get`` / ``put`` / ``stats`` / ``len``)
so the source layer treats them interchangeably.
"""

from __future__ import annotations

import fakeredis
import pandas as pd
import pytest

from promanomaly.cache import RedisQueryCache
from promanomaly.source import QueryResult, QuerySeries
from promanomaly.state import GroupSnapshot, Sample, SharedSnapshotCache, SnapshotStore


@pytest.fixture
def redis_client() -> fakeredis.FakeStrictRedis:
    return fakeredis.FakeStrictRedis()


# ----------------------------------------------------------------------
# RedisQueryCache
# ----------------------------------------------------------------------


def _query_result(values: list[tuple[float, float]] | None = None) -> QueryResult:
    samples = pd.DataFrame(
        values or [(0.0, 1.0), (15.0, 2.0)],
        columns=["timestamp", "y"],
    )
    return QueryResult(series=[QuerySeries(labels={"i": "a"}, samples=samples)])


def test_redis_query_cache_round_trips_a_value(redis_client: fakeredis.FakeStrictRedis) -> None:
    cache: RedisQueryCache[tuple[str, int], QueryResult] = RedisQueryCache(
        client=redis_client, ttl_seconds=30.0
    )
    payload = _query_result()
    cache.put(("k", 1), payload, now=0.0)
    out = cache.get(("k", 1), now=0.0)
    assert out is not None
    assert out.series[0].labels == {"i": "a"}
    # DataFrame round-trips with same column names + values.
    assert list(out.series[0].samples.columns) == ["timestamp", "y"]
    assert out.series[0].samples["y"].tolist() == [1.0, 2.0]
    assert cache.stats.hits == 1
    assert cache.stats.misses == 0


def test_redis_query_cache_uses_safe_json_not_pickle(
    redis_client: fakeredis.FakeStrictRedis,
) -> None:
    """Wire-format guard: the Redis-stored payload must be JSON, never
    pickle. The whole point of the v0.4 redo was so a write-capable but
    untrusted Redis can't deserialise to RCE — pickle would silently
    bring that back."""
    import json

    cache: RedisQueryCache[tuple[str, int], QueryResult] = RedisQueryCache(
        client=redis_client, ttl_seconds=30.0, key_prefix="raw:test"
    )
    cache.put(("k", 1), _query_result(), now=0.0)
    raw = redis_client.get(f"raw:test:{('k', 1)!r}")
    assert raw is not None
    decoded = json.loads(raw)
    assert decoded["v"] == 1
    assert decoded["series"][0]["labels"] == {"i": "a"}


def test_redis_query_cache_miss_when_key_unknown(redis_client: fakeredis.FakeStrictRedis) -> None:
    cache: RedisQueryCache[tuple[str, int], QueryResult] = RedisQueryCache(
        client=redis_client, ttl_seconds=30.0
    )
    assert cache.get(("missing", 0), now=0.0) is None
    assert cache.stats.misses == 1


def test_redis_query_cache_rejects_invalid_ttl(redis_client: fakeredis.FakeStrictRedis) -> None:
    with pytest.raises(ValueError):
        RedisQueryCache(client=redis_client, ttl_seconds=0.0)


def test_redis_query_cache_isolates_via_key_prefix(
    redis_client: fakeredis.FakeStrictRedis,
) -> None:
    # Two caches on the same Redis with different prefixes must not see
    # each other's keys — the prefix is the deployment isolation knob.
    a: RedisQueryCache[str, str] = RedisQueryCache(
        client=redis_client, ttl_seconds=30.0, key_prefix="env_a:query"
    )
    b: RedisQueryCache[str, str] = RedisQueryCache(
        client=redis_client, ttl_seconds=30.0, key_prefix="env_b:query"
    )
    a.put("k", "from_a", now=0.0)
    assert b.get("k", now=0.0) is None


def test_redis_query_cache_corrupt_entry_is_treated_as_miss(
    redis_client: fakeredis.FakeStrictRedis,
) -> None:
    cache: RedisQueryCache[str, QueryResult] = RedisQueryCache(
        client=redis_client, ttl_seconds=30.0, key_prefix="t"
    )
    # Write a garbage payload bypassing the cache so JSON decode fails.
    redis_client.set(f"t:{'k'!r}", b"not-json")
    assert cache.get("k", now=0.0) is None
    # Stats: hit was counted, then the value was rejected as
    # ``expirations`` once decoding failed.
    assert cache.stats.expirations == 1


def test_redis_query_cache_len_is_throttled(
    redis_client: fakeredis.FakeStrictRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bug #9: ``__len__`` used to SCAN every key on every /metrics
    render. With the throttle, repeat calls within the refresh window
    serve the cached count without another round-trip to Redis."""
    cache: RedisQueryCache[str, QueryResult] = RedisQueryCache(
        client=redis_client, ttl_seconds=30.0, key_prefix="throttle"
    )
    cache.put("k1", _query_result(), now=0.0)
    cache.put("k2", _query_result(), now=0.0)
    assert len(cache) == 2

    # Reach in and add a key directly. Without throttling, len() would
    # immediately see 3; with throttling, it stays cached at 2 until the
    # refresh window elapses.
    redis_client.set("throttle:directly", b"{}")
    assert len(cache) == 2

    # Advance the throttle clock by enough that the SCAN refresh fires.
    cache._cached_size_at = 0.0  # type: ignore[reportPrivateUsage]
    assert len(cache) == 3


# ----------------------------------------------------------------------
# SharedSnapshotCache
# ----------------------------------------------------------------------


def _snapshot(group: str, value: float) -> GroupSnapshot:
    return GroupSnapshot(
        group=group,
        timestamp=123.0,
        samples=[Sample(metric="anomaly_score", labels=(("group", group),), value=value)],
    )


def test_shared_snapshot_cache_publish_and_load_all(
    redis_client: fakeredis.FakeStrictRedis,
) -> None:
    shared = SharedSnapshotCache(client=redis_client, ttl_seconds=60.0)
    shared.publish(_snapshot("g1", 1.0))
    shared.publish(_snapshot("g2", 2.0))
    loaded = sorted(shared.load_all(), key=lambda s: s.group)
    assert [s.group for s in loaded] == ["g1", "g2"]
    assert loaded[0].samples[0].value == 1.0


def test_shared_snapshot_cache_overwrites_on_republish(
    redis_client: fakeredis.FakeStrictRedis,
) -> None:
    shared = SharedSnapshotCache(client=redis_client, ttl_seconds=60.0)
    shared.publish(_snapshot("g1", 1.0))
    shared.publish(_snapshot("g1", 99.0))
    loaded = shared.load_all()
    assert len(loaded) == 1
    assert loaded[0].samples[0].value == 99.0


def test_shared_snapshot_cache_remove_drops_entry(
    redis_client: fakeredis.FakeStrictRedis,
) -> None:
    shared = SharedSnapshotCache(client=redis_client, ttl_seconds=60.0)
    shared.publish(_snapshot("g1", 1.0))
    shared.remove("g1")
    assert shared.load_all() == []


def test_follower_syncer_populates_local_store(
    redis_client: fakeredis.FakeStrictRedis,
) -> None:
    from promanomaly.state import FollowerSyncer

    shared = SharedSnapshotCache(client=redis_client, ttl_seconds=60.0)
    shared.publish(_snapshot("g1", 1.0))
    shared.publish(_snapshot("g2", 2.0))
    store = SnapshotStore()
    syncer = FollowerSyncer(store=store, shared=shared)
    count = syncer.poll_once()
    assert count == 2
    snap = store.get("g1")
    assert snap is not None
    assert snap.samples[0].value == 1.0
