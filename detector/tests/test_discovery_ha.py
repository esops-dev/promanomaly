"""HA-correct discovery-absence tracking.

The in-memory :class:`DiscoveryMissTracker` resets on Lease failover, so
a series that went missing right before a handover would restart its
``expect_grace_runs`` window on the new leader. The Redis-backed
:class:`RedisDiscoveryMissTracker` keeps the per-series miss counts in
the shared cache so a newly-elected leader inherits them.

These tests exercise the Redis tracker against ``fakeredis`` and assert
parity with the in-memory tracker's semantics plus the failover-survival
property that is the whole point of the feature.
"""

from __future__ import annotations

import fakeredis
import pytest

from promanomaly.state import SnapshotStore
from promanomaly.state.discovery import (
    DiscoveryMissTracker,
    RedisDiscoveryMissTracker,
    _decode_series_key,
    _encode_series_key,
)

A = (("instance", "a"),)
B = (("instance", "b"),)


@pytest.fixture
def redis_client() -> fakeredis.FakeStrictRedis:
    return fakeredis.FakeStrictRedis()


def test_series_key_round_trips() -> None:
    key = (("instance", "node-17"), ("job", "node"))
    assert _decode_series_key(_encode_series_key(key)) == key


def test_decode_rejects_corrupt_field() -> None:
    assert _decode_series_key("not json") is None
    assert _decode_series_key('{"not": "a list"}') is None
    assert _decode_series_key('[["k"]]') is None  # wrong pair arity


def test_redis_tracker_marks_blocking_io(redis_client: fakeredis.FakeStrictRedis) -> None:
    tracker = RedisDiscoveryMissTracker(client=redis_client, ttl_seconds=60.0)
    assert tracker.is_blocking_io is True
    assert DiscoveryMissTracker().is_blocking_io is False


def test_redis_tracker_transitions_at_grace(redis_client: fakeredis.FakeStrictRedis) -> None:
    tracker = RedisDiscoveryMissTracker(client=redis_client, ttl_seconds=60.0)
    # Run 1: A and B both present — no transitions, both known.
    assert tracker.record_observation("g", "q", [A, B], grace_runs=2) == []
    assert tracker.known_series("g", "q") == {A, B}
    # Run 2: B missing once (below grace).
    assert tracker.record_observation("g", "q", [A], grace_runs=2) == []
    assert tracker.known_missing("g", "q", 2) == []
    # Run 3: B missing twice — crosses grace_runs and transitions.
    assert tracker.record_observation("g", "q", [A], grace_runs=2) == [B]
    assert tracker.known_missing("g", "q", 2) == [B]


def test_redis_tracker_reset_on_reappearance(redis_client: fakeredis.FakeStrictRedis) -> None:
    tracker = RedisDiscoveryMissTracker(client=redis_client, ttl_seconds=60.0)
    tracker.record_observation("g", "q", [A], grace_runs=1)
    tracker.record_observation("g", "q", [], grace_runs=1)
    assert tracker.known_missing("g", "q", 1) == [A]
    # A reappears — its miss count resets, so it is no longer missing.
    tracker.record_observation("g", "q", [A], grace_runs=1)
    assert tracker.known_missing("g", "q", 1) == []


def test_redis_tracker_forget_after_runs_bounds_state(
    redis_client: fakeredis.FakeStrictRedis,
) -> None:
    tracker = RedisDiscoveryMissTracker(client=redis_client, ttl_seconds=60.0)
    tracker.record_observation("g", "q", [A], grace_runs=1)
    # Miss A three times with forget_after_runs=2 — after the 3rd it is
    # dropped entirely and treated as a fresh discovery if it returns.
    tracker.record_observation("g", "q", [], grace_runs=1, forget_after_runs=2)  # miss 1
    tracker.record_observation("g", "q", [], grace_runs=1, forget_after_runs=2)  # miss 2
    assert A in tracker.known_series("g", "q")
    tracker.record_observation(
        "g", "q", [], grace_runs=1, forget_after_runs=2
    )  # miss 3 -> forgotten
    assert tracker.known_series("g", "q") == set()


def test_redis_tracker_survives_failover(redis_client: fakeredis.FakeStrictRedis) -> None:
    """The failover property: a second tracker pointed at the same Redis
    inherits the first leader's miss counts rather than starting at zero."""
    leader1 = RedisDiscoveryMissTracker(client=redis_client, ttl_seconds=60.0)
    leader1.record_observation("g", "q", [A], grace_runs=3)
    leader1.record_observation("g", "q", [], grace_runs=3)  # miss 1
    leader1.record_observation("g", "q", [], grace_runs=3)  # miss 2

    # Lease failover: a brand-new tracker (new leader process) reads the
    # same Redis. One more missing run crosses grace=3 — the count was
    # inherited, not reset.
    leader2 = RedisDiscoveryMissTracker(client=redis_client, ttl_seconds=60.0)
    assert leader2.record_observation("g", "q", [], grace_runs=3) == [A]


def test_redis_tracker_drop_group(redis_client: fakeredis.FakeStrictRedis) -> None:
    tracker = RedisDiscoveryMissTracker(client=redis_client, ttl_seconds=60.0)
    tracker.record_observation("g1", "q", [A], grace_runs=1)
    tracker.record_observation("g2", "q", [B], grace_runs=1)
    tracker.drop_group("g1")
    assert tracker.known_series("g1", "q") == set()
    # g2 is untouched.
    assert tracker.known_series("g2", "q") == {B}


def test_redis_tracker_sets_ttl(redis_client: fakeredis.FakeStrictRedis) -> None:
    tracker = RedisDiscoveryMissTracker(
        client=redis_client, ttl_seconds=120.0, key_prefix="pre:discovery"
    )
    tracker.record_observation("g", "q", [A], grace_runs=1)
    ttl = redis_client.ttl("pre:discovery:g:q")
    assert 0 < ttl <= 120


def test_store_swaps_tracker_and_reports_blocking(
    redis_client: fakeredis.FakeStrictRedis,
) -> None:
    store = SnapshotStore()
    assert store.discovery_blocking_io is False
    tracker = RedisDiscoveryMissTracker(client=redis_client, ttl_seconds=60.0)
    store.set_discovery_tracker(tracker)
    assert store.discovery_blocking_io is True
    # And the store delegates to the swapped tracker.
    store.record_discovery_observation("g", "q", [A], 1)
    assert store.discovery_known_series("g", "q") == {A}


def test_redis_tracker_degrades_on_redis_error() -> None:
    """A Redis outage must not crash the detection run — the tracker
    degrades to "no observation recorded / nothing missing" instead."""

    class _BrokenRedis:
        def hgetall(self, *_a: object, **_k: object) -> dict[str, str]:
            raise ConnectionError("redis down")

        def pipeline(self) -> object:
            raise ConnectionError("redis down")

        def scan_iter(self, *_a: object, **_k: object) -> list[str]:
            raise ConnectionError("redis down")

    tracker = RedisDiscoveryMissTracker(client=_BrokenRedis(), ttl_seconds=60.0)
    # No exception bubbles out; the run continues with degraded tracking.
    assert tracker.record_observation("g", "q", [A], grace_runs=1) == []
    assert tracker.known_missing("g", "q", 1) == []
    tracker.drop_group("g")
