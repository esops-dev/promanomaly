"""Consistent-hash series sharding for active/active scale-out.

In scale-out mode (``scaleOut.enabled``) ``N`` replicas each own a
disjoint slice of the series and run concurrently. Ownership is decided
by **rendezvous hashing** (a.k.a. highest-random-weight, HRW): for a
given series key every shard computes a deterministic weight and the
highest-weighted shard owns the series. Compared with plain
``hash(key) % shards`` this keeps resharding on a shard-count change to
roughly ``1/shards`` of the keys moving — the "minimal movement"
property the roadmap asks for — without a ring or virtual nodes.

The hash must be **stable across processes**: Python's built-in ``hash``
is salted per interpreter (``PYTHONHASHSEED``), so two replicas would
disagree on ownership. We use BLAKE2b over the canonical series-key
bytes instead, which every replica computes identically.

Sharding is a *partition* of the existing work, never a multiplier:
each series is owned by exactly one shard, so the union of the shards'
output is the whole estate with no duplication and no new cardinality.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass

# A StatefulSet pod is named ``<statefulset>-<ordinal>``; the trailing
# integer is the stable shard index. Matches the longest trailing run of
# digits so a release name containing digits (``promanomaly2-0``) still
# resolves the ordinal correctly.
_ORDINAL_RE = re.compile(r"-(\d+)$")

# Series labels travel as a sorted tuple of (name, value) pairs
# (``QuerySeries.label_key``). The canonical byte encoding joins them
# with control characters that cannot appear in a Prometheus label name
# or value, so two distinct label sets can never collide on the same
# bytes.
_PAIR_SEP = "\x1f"  # unit separator, between name and value
_LABEL_SEP = "\x1e"  # record separator, between pairs


def _key_bytes(series_key: tuple[tuple[str, str], ...]) -> bytes:
    """Canonical, process-stable byte encoding of a series label key."""
    return _LABEL_SEP.join(f"{name}{_PAIR_SEP}{value}" for name, value in series_key).encode(
        "utf-8"
    )


def _weight(key_bytes: bytes, shard_index: int) -> int:
    """Rendezvous weight of ``shard_index`` for a series, as a 64-bit int.

    BLAKE2b is keyed by the shard index so the per-(series, shard)
    weights are independent draws; the highest wins. Deterministic and
    identical on every replica.
    """
    digest = hashlib.blake2b(key_bytes, digest_size=8, salt=shard_index.to_bytes(8, "big")).digest()
    return int.from_bytes(digest, "big")


@dataclass(frozen=True)
class ShardAssignment:
    """This replica's place in the shard ring.

    ``index`` is the shard this replica owns (``0 <= index < count``);
    ``count`` is the total number of shards. A single-shard assignment
    (``count == 1``) owns everything, so the hashing fast-path short
    circuits to ``True``.
    """

    index: int
    count: int

    def __post_init__(self) -> None:
        if self.count < 1:
            raise ValueError(f"shard count must be >= 1, got {self.count}")
        if not 0 <= self.index < self.count:
            raise ValueError(f"shard index {self.index} out of range for count {self.count}")

    def owns(self, series_key: tuple[tuple[str, str], ...]) -> bool:
        """Return whether this shard owns ``series_key`` under HRW hashing.

        Ties (identical weights — astronomically unlikely with a 64-bit
        digest) break toward the lower shard index so every replica
        resolves them identically.
        """
        if self.count == 1:
            return True
        key_bytes = _key_bytes(series_key)
        best_index = 0
        best_weight = _weight(key_bytes, 0)
        for candidate in range(1, self.count):
            weight = _weight(key_bytes, candidate)
            if weight > best_weight:
                best_weight = weight
                best_index = candidate
        return best_index == self.index


def resolve_shard_index(configured: int | None, *, shards: int) -> int:
    """Pick this replica's shard index.

    Order: explicit ``scaleOut.shard_index`` > the StatefulSet pod
    ordinal parsed from ``POD_NAME`` (the trailing ``-N``) > ``0``. The
    chart injects ``POD_NAME`` via the Downward API for every replica, so
    a StatefulSet's pods self-assign ``0..shards-1`` with no operator
    action. Raises ``ValueError`` when the resolved index falls outside
    ``[0, shards)`` so a misconfiguration fails loudly at boot rather
    than silently scoring the wrong slice.
    """
    index = configured
    if index is None:
        index = _ordinal_from_env()
    if index is None:
        index = 0
    if not 0 <= index < shards:
        raise ValueError(
            f"resolved shard index {index} out of range for scaleOut.shards={shards}; "
            "set scaleOut.shard_index explicitly or run under a StatefulSet so POD_NAME "
            "carries a valid ordinal"
        )
    return index


def _ordinal_from_env() -> int | None:
    """Parse the StatefulSet ordinal from ``POD_NAME``, or ``None``."""
    pod_name = os.environ.get("POD_NAME")
    if not pod_name:
        return None
    match = _ORDINAL_RE.search(pod_name)
    if match is None:
        return None
    return int(match.group(1))


__all__ = ["ShardAssignment", "resolve_shard_index"]
