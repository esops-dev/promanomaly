"""Tests for the single-replica LRU query cache."""

from __future__ import annotations

import httpx
import pytest

from promanomaly.cache import TTLCache
from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.exporter import Exporter, OperationalMetrics
from promanomaly.source import PromQLSource, QueryResult, QuerySeries
from promanomaly.state import SnapshotStore

# ----------------------------------------------------------------------
# TTLCache unit tests
# ----------------------------------------------------------------------


def test_ttl_cache_hit_and_miss() -> None:
    cache: TTLCache[str, int] = TTLCache(max_entries=4, ttl_seconds=10.0)
    assert cache.get("a", now=0.0) is None
    assert cache.stats.misses == 1
    cache.put("a", 1, now=0.0)
    assert cache.get("a", now=1.0) == 1
    assert cache.stats.hits == 1


def test_ttl_cache_expires_after_ttl() -> None:
    cache: TTLCache[str, int] = TTLCache(max_entries=4, ttl_seconds=1.0)
    cache.put("a", 1, now=0.0)
    # Within TTL — still a hit.
    assert cache.get("a", now=0.5) == 1
    # Beyond TTL — expired, counted as a miss + expiration.
    assert cache.get("a", now=2.0) is None
    assert cache.stats.expirations == 1


def test_ttl_cache_lru_evicts_least_recently_used() -> None:
    cache: TTLCache[str, int] = TTLCache(max_entries=2, ttl_seconds=60.0)
    cache.put("a", 1, now=0.0)
    cache.put("b", 2, now=0.0)
    # Touch a so b becomes the LRU.
    assert cache.get("a", now=0.0) == 1
    cache.put("c", 3, now=0.0)
    assert cache.get("b", now=0.0) is None
    assert cache.stats.evictions == 1
    assert cache.get("a", now=0.0) == 1
    assert cache.get("c", now=0.0) == 3


def test_ttl_cache_rejects_zero_max_entries_or_ttl() -> None:
    with pytest.raises(ValueError):
        TTLCache(max_entries=0, ttl_seconds=1.0)
    with pytest.raises(ValueError):
        TTLCache(max_entries=1, ttl_seconds=0.0)


# ----------------------------------------------------------------------
# PromQLSource integration tests
# ----------------------------------------------------------------------


class _FakeTransport(httpx.MockTransport):
    """httpx mock that records the requests it sees + returns a canned payload."""

    def __init__(self) -> None:
        self.calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            self.calls += 1
            return httpx.Response(
                200,
                json={
                    "status": "success",
                    "data": {
                        "resultType": "matrix",
                        "result": [
                            {
                                "metric": {"instance": "h"},
                                "values": [
                                    [0.0, "1.0"],
                                    [15.0, "2.0"],
                                ],
                            }
                        ],
                    },
                },
            )

        super().__init__(handler)


def _make_source(
    cache: TTLCache[tuple[str, float, float, int, int], QueryResult] | None,
) -> tuple[PromQLSource, _FakeTransport]:
    transport = _FakeTransport()
    src = PromQLSource(base_url="http://stub", timeout=1.0, cache=cache)
    # Swap the client out for one talking to the mock transport.
    src._client = httpx.AsyncClient(transport=transport, base_url="http://stub")
    return src, transport


@pytest.mark.asyncio
async def test_source_serves_cached_response_inside_ttl() -> None:
    cache: TTLCache[tuple[str, float, float, int, int], QueryResult] = TTLCache(
        max_entries=8, ttl_seconds=60.0
    )
    src, transport = _make_source(cache)
    try:
        first = await src.range_query(
            promql="up", end=100.0, window_seconds=60.0, step_seconds=15.0
        )
        second = await src.range_query(
            promql="up", end=100.0, window_seconds=60.0, step_seconds=15.0
        )
        assert transport.calls == 1, "second call should be served from cache"
        assert first.series and second.series
        assert cache.stats.hits == 1
        assert cache.stats.misses == 1
    finally:
        await src.close()


@pytest.mark.asyncio
async def test_source_quantises_end_to_step_bucket() -> None:
    """Two queries inside one step window collapse onto the same cache key."""
    cache: TTLCache[tuple[str, float, float, int, int], QueryResult] = TTLCache(
        max_entries=8, ttl_seconds=60.0
    )
    src, transport = _make_source(cache)
    try:
        await src.range_query(promql="up", end=100.0, window_seconds=60.0, step_seconds=15.0)
        # ``end`` jitters by a few seconds across overlapping callers in the
        # same scrape bucket — the same cache slot should still serve.
        await src.range_query(promql="up", end=104.5, window_seconds=60.0, step_seconds=15.0)
        assert transport.calls == 1
        # Move beyond the step bucket; cache must miss and refetch.
        await src.range_query(promql="up", end=200.0, window_seconds=60.0, step_seconds=15.0)
        assert transport.calls == 2
    finally:
        await src.close()


@pytest.mark.asyncio
async def test_source_without_cache_always_calls_tsdb() -> None:
    src, transport = _make_source(cache=None)
    try:
        await src.range_query(promql="up", end=100.0, window_seconds=60.0, step_seconds=15.0)
        await src.range_query(promql="up", end=100.0, window_seconds=60.0, step_seconds=15.0)
        assert transport.calls == 2
    finally:
        await src.close()


# ----------------------------------------------------------------------
# Exporter integration: cache stats surface as Prometheus metrics
# ----------------------------------------------------------------------


def _stub_query_result() -> QueryResult:
    import pandas as pd

    df = pd.DataFrame([(0.0, 1.0)], columns=["timestamp", "y"])
    return QueryResult(series=[QuerySeries(labels={"instance": "h"}, samples=df)])


def test_exporter_emits_cache_metrics() -> None:
    cache: TTLCache[tuple[str, float, float, int, int], QueryResult] = TTLCache(
        max_entries=2, ttl_seconds=60.0
    )
    src = PromQLSource(base_url="http://stub", timeout=1.0, cache=cache)
    store = SnapshotStore()
    ops = OperationalMetrics()
    exporter = Exporter(store, ops, src)

    # Simulate: one miss + put, then a hit, then an eviction.
    key = ("up", 60.0, 15.0, 0, 6)
    assert cache.get(key, now=0.0) is None
    cache.put(key, _stub_query_result(), now=0.0)
    assert cache.get(key, now=1.0) is not None

    cache.put(("other", 60.0, 15.0, 0, 6), _stub_query_result(), now=2.0)
    cache.put(("third", 60.0, 15.0, 0, 6), _stub_query_result(), now=3.0)  # evicts oldest

    body, _content_type = exporter.render()
    text = body.decode("utf-8")
    assert "anomaly_query_cache_hits_total" in text
    assert "anomaly_query_cache_misses_total" in text
    assert "anomaly_query_cache_entries" in text

    hits = ops.registry.get_sample_value("anomaly_query_cache_hits_total")
    misses = ops.registry.get_sample_value("anomaly_query_cache_misses_total")
    evictions = ops.registry.get_sample_value("anomaly_query_cache_evictions_total")
    entries = ops.registry.get_sample_value("anomaly_query_cache_entries")
    assert hits == pytest.approx(1.0)
    assert misses == pytest.approx(1.0)
    assert evictions == pytest.approx(1.0)
    # After eviction we have two entries left.
    assert entries == pytest.approx(2.0)


# ----------------------------------------------------------------------
# Config wiring
# ----------------------------------------------------------------------


def test_query_cache_config_defaults_on() -> None:
    cfg = Config.model_validate(
        {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://x"},
            "groups": [
                {
                    "name": "g",
                    "queries": [{"id": "m", "promql": "up", "detectors": [{"name": "MAD"}]}],
                }
            ],
        }
    )
    assert cfg.safety.query_cache.enabled is True
    assert cfg.safety.query_cache.max_entries == 1024
    assert cfg.safety.query_cache.ttl_seconds == 30.0


def test_query_cache_config_can_be_disabled() -> None:
    cfg = Config.model_validate(
        {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://x"},
            "safety": {"query_cache": {"enabled": False, "max_entries": 8, "ttl": "5s"}},
            "groups": [
                {
                    "name": "g",
                    "queries": [{"id": "m", "promql": "up", "detectors": [{"name": "MAD"}]}],
                }
            ],
        }
    )
    assert cfg.safety.query_cache.enabled is False
    assert cfg.safety.query_cache.max_entries == 8
    assert cfg.safety.query_cache.ttl_seconds == 5.0


def test_exporter_handles_source_without_cache() -> None:
    src = PromQLSource(base_url="http://stub", timeout=1.0, cache=None)
    store = SnapshotStore()
    ops = OperationalMetrics()
    exporter = Exporter(store, ops, src)
    # Sanity: render works even when cache is disabled — counters stay at 0.
    body, _ = exporter.render()
    assert b"anomaly_query_cache_hits_total" in body
    hits = ops.registry.get_sample_value("anomaly_query_cache_hits_total")
    assert hits == pytest.approx(0.0)


def test_exporter_handles_cache_replacement_across_reload() -> None:
    """Reload swaps the cache out. The exporter must keep the wire
    counters monotonic AND keep accepting new hits from the fresh cache
    instead of waiting for it to "catch up" to the old cache's totals.
    """
    cache_a: TTLCache[tuple[str, float, float, int, int], QueryResult] = TTLCache(
        max_entries=8, ttl_seconds=60.0
    )
    src = PromQLSource(base_url="http://stub", timeout=1.0, cache=cache_a)
    store = SnapshotStore()
    ops = OperationalMetrics()
    exporter = Exporter(store, ops, src)

    # First render: zero stats land cleanly.
    exporter.render()

    # Accumulate some traffic on cache A.
    cache_a.put(("a", 60.0, 15.0, 0, 6), _stub_query_result(), now=0.0)
    for _ in range(5):
        cache_a.get(("a", 60.0, 15.0, 0, 6), now=1.0)  # 5 hits
    exporter.render()
    assert ops.registry.get_sample_value("anomaly_query_cache_hits_total") == pytest.approx(5.0)

    # Simulate a config reload that swaps cache A out for a fresh B.
    cache_b: TTLCache[tuple[str, float, float, int, int], QueryResult] = TTLCache(
        max_entries=8, ttl_seconds=60.0
    )
    src.replace_cache(cache_b)

    # Drive a couple of hits on B (its stats start at 0, lower than A's).
    cache_b.put(("a", 60.0, 15.0, 0, 6), _stub_query_result(), now=0.0)
    for _ in range(2):
        cache_b.get(("a", 60.0, 15.0, 0, 6), now=1.0)
    exporter.render()
    # Counter must reflect A's 5 hits PLUS B's 2 hits = 7. Pre-fix
    # behaviour would freeze at 5 until B accumulated past 5.
    assert ops.registry.get_sample_value("anomaly_query_cache_hits_total") == pytest.approx(7.0)

    # Further hits on B keep adding cleanly.
    for _ in range(3):
        cache_b.get(("a", 60.0, 15.0, 0, 6), now=1.0)
    exporter.render()
    assert ops.registry.get_sample_value("anomaly_query_cache_hits_total") == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_subsecond_step_does_not_overcollapse_buckets() -> None:
    """The cache key quantises ``end`` to ``step_seconds``. A sub-second
    step like 250ms must produce distinct buckets when ``end`` advances
    by 0.5s — otherwise the cache hands back stale data on
    high-frequency monitoring configs.
    """
    cache: TTLCache[tuple[str, float, float, int, int], QueryResult] = TTLCache(
        max_entries=8, ttl_seconds=60.0
    )
    src, transport = _make_source(cache)
    try:
        await src.range_query(promql="up", end=10.00, window_seconds=5.0, step_seconds=0.25)
        # 0.5s later with a 0.25s step → two buckets advanced; must refetch.
        await src.range_query(promql="up", end=10.50, window_seconds=5.0, step_seconds=0.25)
        assert transport.calls == 2
        # Same bucket (within one step) → cache hit.
        await src.range_query(promql="up", end=10.60, window_seconds=5.0, step_seconds=0.25)
        assert transport.calls == 2
    finally:
        await src.close()
