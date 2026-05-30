"""Configuration schema for the Kubernetes custom-metrics adapter.

The adapter is a *separate*, stateless process from the detector: it reads
the anomaly metrics the detector already wrote to the TSDB and re-serves
them through the Kubernetes external/custom metrics API so existing HPA /
KEDA tooling can consume them. It therefore has its own small config
(its own YAML, its own ConfigMap), reusing the detector's
:class:`~promanomaly.config.DatasourceConfig` and
:class:`~promanomaly.config.AuthConfig` so operators configure TSDB access
the same way in both.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import Field, field_validator, model_validator

from ..config import (
    DatasourceConfig,
    Duration,
    _ModelBase,
    _validate_prom_name,
    parse_duration,
)

# The four anomaly signals the roadmap names as the adapter's surface.
# Severity / density / active-series are sensible scaling and gating
# inputs; outside-threshold is exposed for completeness. Scaling raw on
# anomaly_score is a documented anti-pattern, so it is deliberately absent
# from the default set.
DEFAULT_METRICS: tuple[str, ...] = (
    "anomaly_severity",
    "anomaly_density",
    "anomaly_active_series",
    "anomaly_outside_threshold",
)


class AdapterTLSConfig(_ModelBase):
    """Serving TLS for the aggregated apiserver.

    The Kubernetes aggregation layer only talks to an APIService backend
    over HTTPS, so a serving cert is required in-cluster. Both unset
    (the default) serves plain HTTP — useful only for local testing and
    unit tests; the Helm chart always wires a self-signed pair.
    """

    cert_file: str | None = None
    key_file: str | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.cert_file and self.key_file)


class AdapterConfig(_ModelBase):
    """Top-level adapter config.

    ``metrics`` is the closed set of anomaly metric names the adapter will
    serve; each is validated against the Prometheus-name rules (and the
    no-colons rule) exactly like detector output ids. ``custom_resources``
    lists the Kubernetes object kinds the custom-metrics API associates
    metrics with (mapping the object name to a same-named PromQL label).
    """

    datasource: DatasourceConfig
    # Aggregated apiservers conventionally serve on 6443.
    listen: str = ":6443"
    tls: AdapterTLSConfig = Field(default_factory=AdapterTLSConfig)
    timeout: Duration = "10s"
    metrics: list[str] = Field(default_factory=lambda: list(DEFAULT_METRICS))
    # Object kinds exposed through custom.metrics.k8s.io. The object name
    # is matched against the same-named PromQL label (pod -> pod="...",
    # namespace -> namespace="..."). External metrics need no object and
    # are always served.
    custom_resources: list[str] = Field(default_factory=lambda: ["pods", "namespaces"])

    @property
    def timeout_seconds(self) -> float:
        return parse_duration(self.timeout)

    @field_validator("metrics")
    @classmethod
    def _validate_metric_names(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("adapter.metrics must list at least one metric")
        for name in value:
            _validate_prom_name(name, field="adapter.metrics[]")
        return value

    @model_validator(mode="after")
    def _validate_tls_pair(self) -> AdapterConfig:
        tls = self.tls
        if bool(tls.cert_file) != bool(tls.key_file):
            raise ValueError("adapter.tls requires both cert_file and key_file, or neither")
        return self


def load_adapter_config(path: str | Path) -> AdapterConfig:
    """Parse and validate an adapter YAML config file."""
    raw = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"adapter config {path}: top level must be a mapping")
    return AdapterConfig.model_validate(raw)


__all__ = [
    "DEFAULT_METRICS",
    "AdapterConfig",
    "AdapterTLSConfig",
    "load_adapter_config",
]
