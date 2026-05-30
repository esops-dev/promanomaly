"""FastAPI app implementing the Kubernetes external/custom metrics API.

The Kubernetes aggregation layer (kube-aggregator) registers two
``APIService`` objects pointing at this backend and proxies
``/apis/<group>/<version>/...`` requests to it. This module serves the
discovery documents those services expect plus the value endpoints HPA /
KEDA call:

- ``external.metrics.k8s.io/v1beta1`` — cluster-scoped anomaly signals
  (the recommended path for scaling on ``anomaly_density``).
- ``custom.metrics.k8s.io/v1beta1`` — the same signals associated with
  Kubernetes objects (pods, namespaces) for HPA ``Object``/``Pods`` rules.

Every value is read live from the TSDB via :mod:`.resolver`; the adapter
holds no state.
"""

from __future__ import annotations

import datetime
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, generate_latest

from .config import AdapterConfig
from .resolver import AdapterQueryError, MetricResolver, MetricValue, TSDBClient

logger = structlog.get_logger(__name__)

EXTERNAL_GROUP = "external.metrics.k8s.io"
CUSTOM_GROUP = "custom.metrics.k8s.io"
VERSION = "v1beta1"

# Request outcomes for the adapter's own /metrics. Closed enumeration so
# anomaly_adapter_requests_total stays bounded-cardinality.
_OUTCOME_OK = "ok"
_OUTCOME_NOT_FOUND = "not_found"
_OUTCOME_QUERY_ERROR = "query_error"


class AdapterMetrics:
    """Bounded operational metrics for the adapter process itself.

    Cardinality is bounded: ``api`` is ``external``/``custom``, ``metric``
    is drawn from the configured allow-list, and ``outcome`` is a closed
    enum.
    """

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.requests_total = Counter(
            "anomaly_adapter_requests_total",
            "Adapter metric-API requests by API group, metric, and outcome.",
            ["api", "metric", "outcome"],
            registry=self.registry,
        )
        self.tsdb_failures_total = Counter(
            "anomaly_adapter_tsdb_failures_total",
            "Adapter TSDB instant-query failures by metric.",
            ["metric"],
            registry=self.registry,
        )

    def record(self, api: str, metric: str, outcome: str) -> None:
        self.requests_total.labels(api=api, metric=metric, outcome=outcome).inc()
        if outcome == _OUTCOME_QUERY_ERROR:
            self.tsdb_failures_total.labels(metric=metric).inc()

    def render(self) -> bytes:
        return generate_latest(self.registry)


# Map a custom-metrics resource path segment to (Kind, PromQL label the
# object name is matched against, describedObject apiVersion). Core-group
# kinds report "v1"; grouped kinds report "<group>/v1".
_RESOURCE_KIND = {
    "pods": ("Pod", "pod", "v1"),
    "namespaces": ("Namespace", "namespace", "v1"),
    "nodes": ("Node", "node", "v1"),
    "services": ("Service", "service", "v1"),
    "deployments": ("Deployment", "deployment", "apps/v1"),
}


def format_quantity(value: float) -> str:
    """Render a float as a Kubernetes resource.Quantity string.

    Integers render without a decimal point; fractional values render as a
    trimmed fixed-point decimal (never scientific notation, which Quantity
    rejects).
    """
    if value == int(value):
        return str(int(value))
    text = f"{value:.6f}".rstrip("0").rstrip(".")
    return text or "0"


def _rfc3339(timestamp: float) -> str:
    dt = datetime.datetime.fromtimestamp(timestamp, tz=datetime.UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_adapter_app(config: AdapterConfig, resolver: MetricResolver | None = None) -> FastAPI:
    """Build the adapter FastAPI app.

    ``resolver`` may be injected (tests); otherwise a TSDB-backed resolver
    is built from ``config`` and its client lifecycle is managed by the
    app's lifespan.
    """
    owns_client = resolver is None
    client = TSDBClient(
        base_url=config.datasource.url,
        timeout=config.timeout_seconds,
        auth=config.datasource.auth,
    )
    active_resolver = resolver or MetricResolver(client, config.metrics)
    metrics = AdapterMetrics()

    async def lifespan(_: FastAPI) -> Any:
        if owns_client:
            await client.start()
        try:
            yield
        finally:
            if owns_client:
                await client.aclose()

    app = FastAPI(lifespan=lifespan, title="promanomaly-metrics-adapter")

    # ------------------------------------------------------------------
    # Health / liveness / readiness (kubelet + aggregation layer).
    # ------------------------------------------------------------------
    @app.get("/healthz", response_class=PlainTextResponse)
    @app.get("/livez", response_class=PlainTextResponse)
    @app.get("/readyz", response_class=PlainTextResponse)
    async def health() -> str:
        return "ok"

    @app.get("/metrics")
    async def adapter_metrics() -> Response:
        return Response(content=metrics.render(), media_type=CONTENT_TYPE_LATEST)

    # ------------------------------------------------------------------
    # Top-level discovery. kube-aggregator merges these with the
    # APIService registrations.
    # ------------------------------------------------------------------
    @app.get("/apis")
    async def api_groups() -> JSONResponse:
        return JSONResponse(
            {
                "kind": "APIGroupList",
                "apiVersion": "v1",
                "groups": [_api_group(EXTERNAL_GROUP), _api_group(CUSTOM_GROUP)],
            }
        )

    @app.get("/apis/{group}")
    async def api_group(group: str) -> Response:
        if group not in (EXTERNAL_GROUP, CUSTOM_GROUP):
            return _not_found(f"group {group!r}")
        return JSONResponse(_api_group(group))

    # ------------------------------------------------------------------
    # external.metrics.k8s.io
    # ------------------------------------------------------------------
    @app.get(f"/apis/{EXTERNAL_GROUP}/{VERSION}")
    async def external_resources() -> JSONResponse:
        resources = [
            {
                "name": metric,
                "singularName": "",
                "namespaced": True,
                "kind": "ExternalMetricValueList",
                "verbs": ["get"],
            }
            for metric in config.metrics
        ]
        return JSONResponse(
            {
                "kind": "APIResourceList",
                "apiVersion": "v1",
                "groupVersion": f"{EXTERNAL_GROUP}/{VERSION}",
                "resources": resources,
            }
        )

    @app.get(f"/apis/{EXTERNAL_GROUP}/{VERSION}/namespaces/{{namespace}}/{{metric}}")
    async def external_metric(namespace: str, metric: str, request: Request) -> Response:
        if not active_resolver.supports(metric):
            metrics.record("external", metric, _OUTCOME_NOT_FOUND)
            return _not_found(f"external metric {metric!r}")
        selector = request.query_params.get("labelSelector")
        try:
            values = await active_resolver.resolve(metric, label_selector=selector)
        except AdapterQueryError as exc:
            metrics.record("external", metric, _OUTCOME_QUERY_ERROR)
            return _query_error(exc)
        metrics.record("external", metric, _OUTCOME_OK)
        items = [_external_item(metric, v) for v in values]
        return JSONResponse(
            {
                "kind": "ExternalMetricValueList",
                "apiVersion": f"{EXTERNAL_GROUP}/{VERSION}",
                "metadata": {},
                "items": items,
            }
        )

    # ------------------------------------------------------------------
    # custom.metrics.k8s.io
    # ------------------------------------------------------------------
    @app.get(f"/apis/{CUSTOM_GROUP}/{VERSION}")
    async def custom_resources() -> JSONResponse:
        resources = [
            {
                "name": f"{resource}/{metric}",
                "singularName": "",
                "namespaced": True,
                "kind": "MetricValueList",
                "verbs": ["get"],
            }
            for resource in config.custom_resources
            for metric in config.metrics
        ]
        return JSONResponse(
            {
                "kind": "APIResourceList",
                "apiVersion": "v1",
                "groupVersion": f"{CUSTOM_GROUP}/{VERSION}",
                "resources": resources,
            }
        )

    async def _serve_custom(
        resource: str, name: str, metric: str, namespace: str | None, selector: str | None
    ) -> Response:
        if resource not in config.custom_resources:
            return _not_found(f"custom resource {resource!r}")
        if not active_resolver.supports(metric):
            metrics.record("custom", metric, _OUTCOME_NOT_FOUND)
            return _not_found(f"custom metric {metric!r}")
        kind, label, api_version = _RESOURCE_KIND.get(
            resource, (resource.rstrip("s").title(), resource, "v1")
        )
        matchers: list[str] = []
        if namespace is not None and resource != "namespaces":
            matchers.append(f'namespace="{_escape(namespace)}"')
        if name != "*":
            matchers.append(f'{label}="{_escape(name)}"')
        try:
            values = await active_resolver.resolve(
                metric, label_selector=selector, extra_matchers=",".join(matchers)
            )
        except AdapterQueryError as exc:
            metrics.record("custom", metric, _OUTCOME_QUERY_ERROR)
            return _query_error(exc)
        metrics.record("custom", metric, _OUTCOME_OK)
        items = [_custom_item(metric, kind, api_version, namespace, label, name, v) for v in values]
        return JSONResponse(
            {
                "kind": "MetricValueList",
                "apiVersion": f"{CUSTOM_GROUP}/{VERSION}",
                "metadata": {},
                "items": items,
            }
        )

    # Namespaced object metrics (pods, namespaces, ...).
    @app.get(
        f"/apis/{CUSTOM_GROUP}/{VERSION}/namespaces/{{namespace}}/{{resource}}/{{name}}/{{metric}}"
    )
    async def custom_metric_namespaced(
        namespace: str, resource: str, name: str, metric: str, request: Request
    ) -> Response:
        return await _serve_custom(
            resource, name, metric, namespace, request.query_params.get("labelSelector")
        )

    # Cluster-scoped object metrics (e.g. nodes). Fewer path segments, so
    # it never shadows the namespaced route above.
    @app.get(f"/apis/{CUSTOM_GROUP}/{VERSION}/{{resource}}/{{name}}/{{metric}}")
    async def custom_metric_cluster(
        resource: str, name: str, metric: str, request: Request
    ) -> Response:
        return await _serve_custom(
            resource, name, metric, None, request.query_params.get("labelSelector")
        )

    return app


# ----------------------------------------------------------------------
# Response builders
# ----------------------------------------------------------------------
def _api_group(group: str) -> dict[str, Any]:
    gv = {"groupVersion": f"{group}/{VERSION}", "version": VERSION}
    return {
        "kind": "APIGroup",
        "apiVersion": "v1",
        "name": group,
        "versions": [gv],
        "preferredVersion": gv,
    }


def _external_item(metric: str, value: MetricValue) -> dict[str, Any]:
    return {
        "metricName": metric,
        "metricLabels": value.labels,
        "timestamp": _rfc3339(value.timestamp),
        "value": format_quantity(value.value),
    }


def _custom_item(
    metric: str,
    kind: str,
    api_version: str,
    namespace: str | None,
    label: str,
    requested_name: str,
    value: MetricValue,
) -> dict[str, Any]:
    # Prefer the object name carried on the series; fall back to the
    # requested name (single-object queries).
    object_name = value.labels.get(label, requested_name if requested_name != "*" else "")
    described: dict[str, Any] = {
        "kind": kind,
        "name": object_name,
        "apiVersion": api_version,
    }
    # Cluster-scoped objects (e.g. nodes) carry no namespace.
    if namespace is not None:
        described["namespace"] = namespace
    return {
        "describedObject": described,
        "metricName": metric,
        "timestamp": _rfc3339(value.timestamp),
        "value": format_quantity(value.value),
        "selector": None,
    }


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _not_found(what: str) -> JSONResponse:
    return JSONResponse(
        {
            "kind": "Status",
            "apiVersion": "v1",
            "status": "Failure",
            "message": f"{what} not found",
            "reason": "NotFound",
            "code": 404,
        },
        status_code=404,
    )


def _query_error(exc: AdapterQueryError) -> JSONResponse:
    logger.warning("adapter_query_error", error=str(exc))
    return JSONResponse(
        {
            "kind": "Status",
            "apiVersion": "v1",
            "status": "Failure",
            "message": str(exc),
            "reason": "ServiceUnavailable",
            "code": 503,
        },
        status_code=503,
    )


__all__ = ["build_adapter_app", "format_quantity"]
