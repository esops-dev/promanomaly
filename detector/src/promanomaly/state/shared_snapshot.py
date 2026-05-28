"""HA-mode shared snapshot cache + follower sync loop.

The leader publishes each successful group snapshot to Redis; followers
periodically pull every published snapshot via :meth:`load_all` and
feed them into their local :class:`SnapshotStore` so ``/metrics``
serves the same data on every replica.

Snapshot values are JSON-encoded via :mod:`promanomaly.serde` so a
write-capable Redis cannot RCE the readers. Each entry sits behind a
TTL so a stale leader can never leave indefinitely-old data lingering
in the cache; the cooldown ≤ ``refresh_interval / 2`` on demoted
leaders keeps split-brain windows shorter than the TTL so followers
never serve a poisoned snapshot.
"""

from __future__ import annotations

import time
from typing import Any

from .snapshot import GroupSnapshot, SnapshotStore


class SharedSnapshotCache:
    """Redis-backed snapshot cache shared between leader and followers."""

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
        # Local import: serde imports state types, so importing at
        # module top would be circular.
        from ..serde import encode_snapshot

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

        Uses ``SCAN`` to discover keys and ``MGET`` to fetch them in
        one round-trip so followers don't pay one Redis round-trip per
        group per poll. Errors return an empty list; callers fall back
        to whatever they already have rather than blanking the surface.
        Entries that fail to decode (corrupt, wrong-version, non-JSON)
        are skipped silently — same semantics as a missing entry.
        """
        from ..serde import decode_snapshot

        try:
            keys = list(self._client.scan_iter(match=f"{self._prefix}:*", count=200))
            if not keys:
                return []
            raws = self._client.mget(keys)
        except Exception:
            return []
        results: list[GroupSnapshot] = []
        for raw in raws:
            if raw is None:
                continue
            snap = decode_snapshot(raw)
            if snap is not None:
                results.append(snap)
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

    def poll_once(self, now: float | None = None) -> int:
        """Refresh local snapshots from the shared cache. Returns count.

        A snapshot older than ``snapshot_ttl`` is still written so
        ``/metrics`` keeps the wire output visible, but ``_group_ready``
        is flipped to False — a follower whose leader has been failing
        for longer than the TTL must shed traffic, otherwise a rolling
        deploy would gate on data the leader stopped refreshing.

        Clock-skew defence: clamp the staleness comparison to
        ``max(now, snap.timestamp)`` so a follower whose clock has
        drifted *behind* the leader's never marks a fresh snapshot
        stale. Only the opposite drift (follower ahead) can flip
        ``_group_ready`` to False, which is the safer error: a small
        positive skew causes a brief unready window that recovers on
        the next leader publish.
        """
        now_seconds = time.time() if now is None else now
        ttl = self._shared.ttl_seconds
        snapshots = self._shared.load_all()
        for snap in snapshots:
            effective_now = max(now_seconds, snap.timestamp)
            fresh = (snap.timestamp + ttl) >= effective_now
            self._store.write(snap, mark_ready=fresh)
        return len(snapshots)
