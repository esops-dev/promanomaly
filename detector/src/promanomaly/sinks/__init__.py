"""Optional push sinks active alongside the always-on ``/metrics`` exporter.

A sink pushes each successful group snapshot to an external system after
the run completes. One or several sinks may be configured; ``/metrics``
is always available regardless. Failures are isolated and counted on
``anomaly_sink_failures_total`` and never block ``/metrics`` or readiness.
"""

from __future__ import annotations

from ..config import Config, SinkConfig
from ..exporter import OperationalMetrics
from .base import Sink
from .grafana_annotations import GrafanaAnnotationsSink
from .remote_write import RemoteWriteSink


def build_sink(config: SinkConfig | None, operational: OperationalMetrics) -> Sink | None:
    """Construct one configured sink, or ``None`` when pull-only.

    Raises ``ValueError`` if the sink type is unknown — config validation
    already constrains ``type`` to the supported set, so this only trips
    on an internal mismatch.
    """
    if config is None:
        return None
    if config.type == "remote_write":
        assert config.remote_write is not None  # enforced by SinkConfig validation
        return RemoteWriteSink(config.remote_write, operational)
    if config.type == "grafana_annotations":
        assert config.grafana_annotations is not None
        return GrafanaAnnotationsSink(config.grafana_annotations, operational)
    raise ValueError(f"unknown sink type {config.type!r}")


def build_sinks(config: Config, operational: OperationalMetrics) -> list[Sink]:
    """Construct every configured push sink from a :class:`Config`.

    Normalises the scalar ``sink:`` and list ``sinks:`` forms via
    :meth:`Config.effective_sinks`, so the runtime never has to know which
    form the operator used. Returns ``[]`` when pull-only.
    """
    sinks: list[Sink] = []
    for sink_config in config.effective_sinks():
        sink = build_sink(sink_config, operational)
        assert sink is not None  # effective_sinks never yields a None entry
        sinks.append(sink)
    return sinks


__all__ = ["GrafanaAnnotationsSink", "RemoteWriteSink", "Sink", "build_sink", "build_sinks"]
