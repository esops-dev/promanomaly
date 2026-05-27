"""Tests for HA-mode leader election.

The Kubernetes Lease client is mocked so the test suite never reaches
out to a real cluster; the elector's renew / takeover / failover paths
are exercised against the in-memory fake. The tests pin the documented
failover behaviour: a new leader fires its ``on_started_leading``
callback exactly once per acquisition, and a demoted leader fires
``on_stopped_leading`` before any further snapshot writes.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from promanomaly.config import HighAvailabilityConfig
from promanomaly.leader import LeaderElector, LeaseSnapshot, resolve_identity


class _FakeLeaseClient:
    """In-memory stand-in for the real Kubernetes Lease client."""

    def __init__(self) -> None:
        self._snapshot: LeaseSnapshot | None = None

    def get(self) -> LeaseSnapshot | None:
        return self._snapshot

    def create(self, identity: str) -> LeaseSnapshot:
        now = time.time()
        self._snapshot = LeaseSnapshot(
            holder_identity=identity,
            acquire_time=now,
            renew_time=now,
            lease_duration_seconds=15,
            resource_version="1",
            raw=object(),
        )
        return self._snapshot

    def update(self, snapshot: LeaseSnapshot, identity: str, *, renew_only: bool) -> LeaseSnapshot:
        now = time.time()
        new = LeaseSnapshot(
            holder_identity=identity,
            acquire_time=snapshot.acquire_time if renew_only else now,
            renew_time=now,
            lease_duration_seconds=15,
            resource_version=str(int(snapshot.resource_version or "0") + 1),
            raw=object(),
        )
        self._snapshot = new
        return new

    def force(self, snapshot: LeaseSnapshot | None) -> None:
        """Test helper to manipulate the Lease state directly."""
        self._snapshot = snapshot

    def release(self, snapshot: LeaseSnapshot) -> None:
        """Surrender the Lease (graceful release on shutdown)."""
        del snapshot
        if self._snapshot is None:
            return
        # Mirror the production implementation: blank the holder and
        # set renew_time to a past value so followers immediately see
        # the lease as expired.
        self._snapshot = LeaseSnapshot(
            holder_identity="",
            acquire_time=self._snapshot.acquire_time,
            renew_time=time.time() - self._snapshot.lease_duration_seconds * 2,
            lease_duration_seconds=self._snapshot.lease_duration_seconds,
            resource_version=str(int(self._snapshot.resource_version or "0") + 1),
            raw=object(),
        )


def _ha(lease_duration: str = "5s", retry: str = "1s") -> HighAvailabilityConfig:
    return HighAvailabilityConfig.model_validate(
        {
            "enabled": True,
            "lease_name": "test",
            "lease_namespace": "ns",
            "lease_duration": lease_duration,
            "retry_period": retry,
        }
    )


def test_resolve_identity_prefers_configured_value() -> None:
    assert resolve_identity("explicit") == "explicit"


def test_resolve_identity_uses_pod_name_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POD_NAME", "pod-1")
    assert resolve_identity(None) == "pod-1"


@pytest.mark.asyncio
async def test_elector_fires_started_leading_once_on_acquire() -> None:
    client = _FakeLeaseClient()
    started: list[str] = []
    stopped: list[str] = []
    elector = LeaderElector(
        ha=_ha(),
        identity="a",
        client=client,  # type: ignore[arg-type]
        on_started_leading=lambda: _record(started, "a"),
        on_stopped_leading=lambda: _record(stopped, "a"),
    )
    elector.start()
    await _wait_until(lambda: started, timeout=2.0)
    assert started == ["a"]
    # Renewals must NOT re-fire the started callback. Give the renewal
    # loop a couple of cycles.
    await asyncio.sleep(0.3)
    assert started == ["a"]
    await elector.stop()
    assert stopped == []  # never lost the lease in this scenario


@pytest.mark.asyncio
async def test_elector_drops_leadership_when_lease_taken_over() -> None:
    client = _FakeLeaseClient()
    started: list[str] = []
    stopped: list[str] = []
    elector = LeaderElector(
        ha=_ha(),
        identity="a",
        client=client,  # type: ignore[arg-type]
        on_started_leading=lambda: _record(started, "a"),
        on_stopped_leading=lambda: _record(stopped, "a"),
    )
    elector.start()
    await _wait_until(lambda: started, timeout=2.0)
    # Simulate another replica taking the lease over before its
    # natural expiry window.
    client.force(
        LeaseSnapshot(
            holder_identity="b",
            acquire_time=time.time(),
            renew_time=time.time(),
            lease_duration_seconds=15,
            resource_version="99",
            raw=object(),
        )
    )
    await _wait_until(lambda: stopped, timeout=3.0)
    assert stopped == ["a"]
    assert not elector.is_leader
    await elector.stop()


@pytest.mark.asyncio
async def test_elector_releases_lease_on_graceful_stop() -> None:
    """Debt #7: a leader that calls stop() must surrender the Lease so
    the next replica takes over within one poll interval instead of
    waiting the full ``lease_duration``."""
    client = _FakeLeaseClient()
    started: list[str] = []
    elector = LeaderElector(
        ha=_ha(),
        identity="a",
        client=client,  # type: ignore[arg-type]
        on_started_leading=lambda: _record(started, "a"),
        on_stopped_leading=lambda: _record(started, "stopped"),
    )
    elector.start()
    await _wait_until(lambda: started, timeout=2.0)
    await elector.stop()
    # The released Lease has its holder cleared and a stale renew_time
    # so a fresh follower (different identity) would take it over
    # immediately on the next poll.
    after = client.get()
    assert after is not None
    assert after.holder_identity == ""
    assert after.renew_time is not None
    expiry = after.renew_time + after.lease_duration_seconds
    assert expiry < time.time()


@pytest.mark.asyncio
async def test_elector_takes_over_expired_lease() -> None:
    client = _FakeLeaseClient()
    # Pre-existing expired lease held by "previous".
    client.force(
        LeaseSnapshot(
            holder_identity="previous",
            acquire_time=time.time() - 60,
            renew_time=time.time() - 60,
            lease_duration_seconds=1,
            resource_version="1",
            raw=object(),
        )
    )
    started: list[str] = []
    elector = LeaderElector(
        ha=_ha(retry="0.2s"),
        identity="new",
        client=client,  # type: ignore[arg-type]
        on_started_leading=lambda: _record(started, "new"),
        on_stopped_leading=lambda: _record(started, "stopped"),
    )
    elector.start()
    await _wait_until(lambda: started, timeout=2.0)
    assert started == ["new"]
    await elector.stop()


# ----------------------------------------------------------------------
# Async helpers
# ----------------------------------------------------------------------


async def _record(bucket: list[str], value: str) -> None:
    bucket.append(value)


async def _wait_until(predicate: object, *, timeout: float) -> None:
    """Poll ``predicate`` for up to ``timeout`` seconds."""
    assert callable(predicate)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"predicate never became truthy within {timeout}s")
