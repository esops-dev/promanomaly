"""Push-sink abstraction.

Sinks run *after* a successful group run, in addition to the always-on
``/metrics`` pull exporter. The contract is strict: a sink failure is
isolated and counted on ``anomaly_sink_failures_total`` but never blocks
``/metrics`` or readiness (see the project guide). Concrete sinks
therefore swallow their own errors — :meth:`Sink.emit` must not raise —
and the caller in :mod:`promanomaly.main` keeps an outer guard as
defence in depth.
"""

from __future__ import annotations

import abc

from ..exporter import OperationalMetrics
from ..state import GroupSnapshot

# Bounded reason taxonomy for ``anomaly_sink_failures_total{reason=...}``.
# Keep this tight — the whole point of the metric is bounded cardinality.
REASON_HTTP = "http_error"
REASON_TIMEOUT = "timeout"
REASON_CONNECT = "connect_error"
REASON_SERIALIZE = "serialize_error"
REASON_RATE_LIMITED = "rate_limited"
REASON_EXCEPTION = "exception"


class Sink(abc.ABC):
    """A push destination for anomaly snapshots.

    Each configured sink instance is long-lived: it is shared across the
    scheduler for the process lifetime (several may run at once).
    :meth:`start` opens any network client; :meth:`emit` is invoked once
    per successful group run with that group's snapshot; :meth:`aclose`
    releases resources on shutdown.
    """

    #: Bounded-cardinality identifier used as the ``sink`` label value on
    #: ``anomaly_sink_failures_total``.
    name: str

    def __init__(self, operational: OperationalMetrics) -> None:
        self._ops = operational

    @abc.abstractmethod
    async def start(self) -> None:
        """Open the network client. Idempotent."""

    @abc.abstractmethod
    async def emit(self, snapshot: GroupSnapshot) -> None:
        """Push one group's snapshot. Must never raise."""

    @abc.abstractmethod
    async def aclose(self) -> None:
        """Release resources. Idempotent."""

    def _count_failure(self, reason: str) -> None:
        self._ops.sink_failures_total.labels(sink=self.name, reason=reason).inc()


__all__ = [
    "REASON_CONNECT",
    "REASON_EXCEPTION",
    "REASON_HTTP",
    "REASON_RATE_LIMITED",
    "REASON_SERIALIZE",
    "REASON_TIMEOUT",
    "Sink",
]
