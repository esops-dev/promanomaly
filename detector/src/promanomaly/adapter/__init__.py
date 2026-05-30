"""Kubernetes custom/external metrics adapter.

A stateless, opt-in process that re-serves the anomaly metrics the
detector already wrote to the TSDB through the Kubernetes
``external.metrics.k8s.io`` / ``custom.metrics.k8s.io`` APIs, so existing
HPA and KEDA tooling can consume anomaly signal like any other metric.

promanomaly stays a metrics *provider*; the autoscaler remains the
decision-maker. Shipped via the opt-in ``promanomaly-metrics-adapter``
Helm chart and the ``promanomaly adapter`` CLI subcommand.
"""

from __future__ import annotations

from .app import build_adapter_app
from .config import AdapterConfig, load_adapter_config
from .serve import serve_adapter

__all__ = [
    "AdapterConfig",
    "build_adapter_app",
    "load_adapter_config",
    "serve_adapter",
]
