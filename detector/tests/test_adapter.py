"""Tests for the Kubernetes metrics adapter.

Covers config validation, the PromQL-instant-query client + label-selector
translation, the discovery documents, and the external/custom metric
value endpoints (via FastAPI TestClient with an injected resolver).
"""

from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from promanomaly.adapter.app import build_adapter_app, format_quantity
from promanomaly.adapter.config import AdapterConfig, load_adapter_config
from promanomaly.adapter.resolver import (
    MetricResolver,
    MetricValue,
    TSDBClient,
    selector_to_matchers,
)


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
def test_config_defaults() -> None:
    cfg = AdapterConfig.model_validate({"datasource": {"url": "http://vm:8428/"}})
    assert cfg.listen == ":6443"
    assert "anomaly_density" in cfg.metrics
    assert cfg.tls.enabled is False


def test_config_rejects_colon_metric() -> None:
    with pytest.raises(ValueError, match="colons"):
        AdapterConfig.model_validate(
            {"datasource": {"url": "http://vm/"}, "metrics": ["anomaly:bad"]}
        )


def test_config_requires_both_tls_files() -> None:
    with pytest.raises(ValueError, match="cert_file and key_file"):
        AdapterConfig.model_validate(
            {"datasource": {"url": "http://vm/"}, "tls": {"cert_file": "/x.crt"}}
        )


def test_load_adapter_config(tmp_path) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "adapter.yaml"
    path.write_text("datasource:\n  url: http://vm:8428/\nmetrics:\n  - anomaly_density\n")
    cfg = load_adapter_config(path)
    assert cfg.metrics == ["anomaly_density"]


# --------------------------------------------------------------------------
# Selector translation + quantity formatting
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "selector,expected",
    [
        (None, ""),
        ("", ""),
        ("group=q", 'group="q"'),
        ("group==q,ns!=kube-system", 'group="q",ns!="kube-system"'),
        ("bare-no-op", ""),
    ],
)
def test_selector_to_matchers(selector, expected) -> None:  # type: ignore[no-untyped-def]
    assert selector_to_matchers(selector) == expected


@pytest.mark.parametrize(
    "value,expected",
    [(5.0, "5"), (0.5, "0.5"), (4.7, "4.7"), (0.0, "0"), (0.25, "0.25")],
)
def test_format_quantity(value, expected) -> None:  # type: ignore[no-untyped-def]
    assert format_quantity(value) == expected


# --------------------------------------------------------------------------
# TSDB instant-query client
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_tsdb_instant_query_parses_vector() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/query"
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {"__name__": "anomaly_density", "group": "q"},
                            "value": [1700000000, "0.4"],
                        }
                    ],
                },
            },
        )

    client = TSDBClient(base_url="http://stub", timeout=1.0)
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://stub"
    )
    values = await client.instant_query("anomaly_density")
    assert len(values) == 1
    assert values[0].labels == {"group": "q"}  # __name__ stripped
    assert values[0].value == pytest.approx(0.4)


@pytest.mark.asyncio
async def test_tsdb_instant_query_skips_non_finite_values() -> None:
    # A NaN/Inf value cannot be a Kubernetes resource.Quantity; rendering
    # one would 500 the whole metrics-API request, so such series are
    # dropped rather than served.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {"__name__": "anomaly_density", "p": "nan"},
                            "value": [1, "NaN"],
                        },
                        {
                            "metric": {"__name__": "anomaly_density", "p": "inf"},
                            "value": [1, "+Inf"],
                        },
                        {"metric": {"__name__": "anomaly_density", "p": "ok"}, "value": [1, "0.7"]},
                    ],
                },
            },
        )

    client = TSDBClient(base_url="http://stub", timeout=1.0)
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://stub"
    )
    values = await client.instant_query("anomaly_density")
    assert [v.labels["p"] for v in values] == ["ok"]


# --------------------------------------------------------------------------
# FastAPI app with an injected resolver
# --------------------------------------------------------------------------
class _FakeResolver(MetricResolver):
    def __init__(self) -> None:
        super().__init__(client=None, allowed_metrics=["anomaly_density", "anomaly_severity"])  # type: ignore[arg-type]
        self.calls: list[tuple[str, str | None, str]] = []

    async def resolve(self, metric, label_selector=None, extra_matchers=""):  # type: ignore[no-untyped-def]
        self.calls.append((metric, label_selector, extra_matchers))
        return [MetricValue(labels={"group": "q", "pod": "p1"}, value=0.4, timestamp=1700000000.0)]


def _client() -> tuple[TestClient, _FakeResolver]:
    cfg = AdapterConfig.model_validate({"datasource": {"url": "http://vm/"}})
    resolver = _FakeResolver()
    app = build_adapter_app(cfg, resolver=resolver)
    return TestClient(app), resolver


def test_health_endpoints() -> None:
    client, _ = _client()
    for path in ("/healthz", "/livez", "/readyz"):
        assert client.get(path).text == "ok"


def test_apis_discovery_lists_both_groups() -> None:
    client, _ = _client()
    body = client.get("/apis").json()
    names = {g["name"] for g in body["groups"]}
    assert names == {"external.metrics.k8s.io", "custom.metrics.k8s.io"}


def test_external_resource_list() -> None:
    client, _ = _client()
    body = client.get("/apis/external.metrics.k8s.io/v1beta1").json()
    assert body["kind"] == "APIResourceList"
    names = {r["name"] for r in body["resources"]}
    assert "anomaly_density" in names


def test_external_metric_value() -> None:
    client, resolver = _client()
    resp = client.get(
        "/apis/external.metrics.k8s.io/v1beta1/namespaces/monitoring/anomaly_density",
        params={"labelSelector": "group=q"},
    )
    body = resp.json()
    assert body["kind"] == "ExternalMetricValueList"
    assert body["items"][0]["metricName"] == "anomaly_density"
    assert body["items"][0]["value"] == "400m" or body["items"][0]["value"] == "0.4"
    assert resolver.calls[0] == ("anomaly_density", "group=q", "")


def test_external_metric_unknown_is_404() -> None:
    client, _ = _client()
    resp = client.get("/apis/external.metrics.k8s.io/v1beta1/namespaces/monitoring/anomaly_score")
    assert resp.status_code == 404


def test_custom_resource_list_pairs_resource_and_metric() -> None:
    client, _ = _client()
    body = client.get("/apis/custom.metrics.k8s.io/v1beta1").json()
    names = {r["name"] for r in body["resources"]}
    assert "pods/anomaly_density" in names


def test_custom_metric_single_object_builds_matcher() -> None:
    client, resolver = _client()
    resp = client.get(
        "/apis/custom.metrics.k8s.io/v1beta1/namespaces/monitoring/pods/p1/anomaly_severity"
    )
    body = resp.json()
    assert body["kind"] == "MetricValueList"
    obj = body["items"][0]["describedObject"]
    assert obj["kind"] == "Pod"
    assert obj["apiVersion"] == "v1"
    assert obj["namespace"] == "monitoring"
    # extra_matchers carries the namespace + pod name match.
    _metric, _sel, extra = resolver.calls[0]
    assert 'namespace="monitoring"' in extra
    assert 'pod="p1"' in extra


def test_custom_metric_cluster_scoped_node() -> None:
    cfg = AdapterConfig.model_validate(
        {"datasource": {"url": "http://vm/"}, "custom_resources": ["nodes"]}
    )
    resolver = _FakeResolver()
    client = TestClient(build_adapter_app(cfg, resolver=resolver))
    resp = client.get("/apis/custom.metrics.k8s.io/v1beta1/nodes/node-17/anomaly_density")
    body = resp.json()
    assert resp.status_code == 200
    obj = body["items"][0]["describedObject"]
    assert obj["kind"] == "Node"
    # Cluster-scoped objects carry no namespace.
    assert "namespace" not in obj
    # Only the node-name matcher, no namespace matcher.
    _metric, _sel, extra = resolver.calls[0]
    assert extra == 'node="node-17"'


def test_metrics_endpoint_counts_requests() -> None:
    client, _ = _client()
    client.get("/apis/external.metrics.k8s.io/v1beta1/namespaces/ns/anomaly_density")
    client.get("/apis/external.metrics.k8s.io/v1beta1/namespaces/ns/anomaly_score")  # not found
    body = client.get("/metrics").text
    assert "anomaly_adapter_requests_total" in body
    assert 'outcome="ok"' in body
    assert 'outcome="not_found"' in body
