"""TSDB-backed resolution of anomaly metrics into k8s metric values.

The adapter answers a metrics-API request by running an *instant* PromQL
query for the requested metric (optionally constrained by a translated
label selector) and returning the matching series. It holds no state —
every request is a fresh read of what the detector already wrote.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import httpx
import structlog

from ..config import AuthConfig
from ..httpauth import build_httpx_auth

logger = structlog.get_logger(__name__)


class AdapterQueryError(RuntimeError):
    """Raised when the TSDB instant query fails or returns an error envelope."""


@dataclass(frozen=True)
class MetricValue:
    """One resolved series: its (non-reserved) labels and latest value."""

    labels: dict[str, str]
    value: float
    timestamp: float


class TSDBClient:
    """Minimal async client for the PromQL instant-query endpoint."""

    def __init__(self, base_url: str, timeout: float, auth: AuthConfig | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._auth = auth or AuthConfig()
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        if self._client is not None:
            return
        kwargs: dict[str, Any] = {
            "timeout": self._timeout,
            "base_url": self._base_url,
        }
        kwargs.update(build_httpx_auth(self._auth))
        self._client = httpx.AsyncClient(**kwargs)

    async def aclose(self) -> None:
        if self._client is None:
            return
        try:
            await self._client.aclose()
        finally:
            self._client = None

    async def instant_query(self, promql: str) -> list[MetricValue]:
        """Run an instant query and return one :class:`MetricValue` per series."""
        if self._client is None:
            await self.start()
        assert self._client is not None
        try:
            response = await self._client.get("/api/v1/query", params={"query": promql})
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise AdapterQueryError(f"instant query failed: {exc}") from exc
        payload = response.json()
        if payload.get("status") != "success":
            raise AdapterQueryError(f"query error: {payload.get('error', 'unknown')}")
        result = payload.get("data", {}).get("result", [])
        values: list[MetricValue] = []
        for series in result:
            metric = dict(series.get("metric", {}))
            metric.pop("__name__", None)
            raw = series.get("value")
            if not raw or len(raw) != 2:
                continue
            try:
                ts = float(raw[0])
                val = float(raw[1])
            except (TypeError, ValueError):
                continue
            # A Kubernetes resource.Quantity cannot represent NaN/±Inf, and
            # rendering one would raise mid-response and surface as a 500 to
            # the HPA. Such a value is never a usable scaling signal, so skip
            # the series rather than fail the whole request.
            if not math.isfinite(val):
                continue
            values.append(MetricValue(labels=metric, value=val, timestamp=ts))
        return values


def selector_to_matchers(label_selector: str | None) -> str:
    """Translate a Kubernetes label selector into PromQL label matchers.

    Supports the equality-based forms HPA / KEDA emit:
    ``key=value`` / ``key==value`` / ``key!=value``, comma-separated.
    Unparseable clauses are skipped with a warning rather than failing the
    whole request — a malformed selector yields a broader query, never a
    500. Set-based selectors (``key in (...)``) are not translated.
    """
    if not label_selector:
        return ""
    matchers: list[str] = []
    for clause in label_selector.split(","):
        clause = clause.strip()
        if not clause:
            continue
        if "!=" in clause:
            key, _, value = clause.partition("!=")
            op = "!="
        elif "==" in clause:
            key, _, value = clause.partition("==")
            op = "="
        elif "=" in clause:
            key, _, value = clause.partition("=")
            op = "="
        else:
            logger.warning("adapter_selector_skipped", clause=clause)
            continue
        key = key.strip()
        value = value.strip().strip('"')
        if not key:
            continue
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        matchers.append(f'{key}{op}"{escaped}"')
    return ",".join(matchers)


class MetricResolver:
    """Resolves a metric name + selector into series values via the TSDB."""

    def __init__(self, client: TSDBClient, allowed_metrics: list[str]) -> None:
        self._client = client
        self._allowed = set(allowed_metrics)

    def supports(self, metric: str) -> bool:
        return metric in self._allowed

    async def resolve(
        self, metric: str, label_selector: str | None = None, extra_matchers: str = ""
    ) -> list[MetricValue]:
        """Resolve ``metric`` constrained by ``label_selector`` and any
        adapter-supplied ``extra_matchers`` (e.g. the object name match for
        custom metrics). Raises :class:`KeyError` for an unsupported metric.
        """
        if metric not in self._allowed:
            raise KeyError(metric)
        selector = selector_to_matchers(label_selector)
        parts = [p for p in (selector, extra_matchers) if p]
        matcher_str = ",".join(parts)
        promql = f"{metric}{{{matcher_str}}}" if matcher_str else metric
        return await self._client.instant_query(promql)


__all__ = [
    "AdapterQueryError",
    "MetricResolver",
    "MetricValue",
    "TSDBClient",
    "selector_to_matchers",
]
