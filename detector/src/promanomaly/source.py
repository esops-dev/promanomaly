"""PromQL range-query client.

Thin wrapper around ``httpx`` that returns one ``QuerySeries`` per series
in the response, parsed into a pandas DataFrame ready for the detectors.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx
import pandas as pd

from .config import AuthConfig


class SourceQueryError(RuntimeError):
    """Raised when the TSDB query fails or returns an error envelope.

    Includes a ``reason`` slug suitable for use as a bounded-cardinality
    label value on operational counters.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(f"{reason}: {message}")
        self.reason = reason
        self.message = message


@dataclass(frozen=True)
class QuerySeries:
    """One time-series result from a PromQL range query."""

    labels: dict[str, str]
    samples: pd.DataFrame  # columns: timestamp (float, s since epoch), y (float)

    @property
    def label_key(self) -> tuple[tuple[str, str], ...]:
        """Hashable, order-stable identity of the series label set."""
        return tuple(sorted(self.labels.items()))


@dataclass
class QueryResult:
    series: list[QuerySeries] = field(default_factory=list)
    truncated: bool = False  # True when max_series_per_query trimmed the result


class PromQLSource:
    """Minimal range-query client.

    Lifecycle: a single instance is shared across the scheduler. Each
    detector run uses :meth:`range_query` to pull one rolling window per
    configured query.
    """

    def __init__(
        self,
        base_url: str,
        timeout: float,
        auth: AuthConfig | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._auth = auth or AuthConfig()
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> PromQLSource:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def start(self) -> None:
        if self._client is not None:
            return
        kwargs: dict[str, Any] = {"timeout": self._timeout, "base_url": self._base_url}
        if self._auth.type == "bearer" and self._auth.token:
            kwargs["headers"] = {"Authorization": f"Bearer {self._auth.token}"}
        elif self._auth.type == "basic" and self._auth.username:
            kwargs["auth"] = httpx.BasicAuth(self._auth.username, self._auth.password or "")
        elif self._auth.type == "mtls":
            cert = self._auth.cert_file
            key = self._auth.key_file
            if cert and key:
                kwargs["cert"] = (cert, key)
            if self._auth.ca_file:
                kwargs["verify"] = self._auth.ca_file
        self._client = httpx.AsyncClient(**kwargs)

    async def close(self) -> None:
        if self._client is None:
            return
        try:
            await self._client.aclose()
        finally:
            self._client = None

    async def range_query(
        self,
        promql: str,
        end: float,
        window_seconds: float,
        step_seconds: float,
        max_series: int | None = None,
    ) -> QueryResult:
        """Run a PromQL range query covering ``[end - window, end]``.

        Raises :class:`SourceQueryError` on any TSDB-side failure.
        Truncation to ``max_series`` is reported via ``QueryResult.truncated``
        rather than failing the call.
        """
        if self._client is None:
            await self.start()
        assert self._client is not None

        start = end - window_seconds
        params = {
            "query": promql,
            "start": f"{start:.3f}",
            "end": f"{end:.3f}",
            "step": f"{step_seconds:.3f}",
        }
        try:
            response = await self._client.get("/api/v1/query_range", params=params)
        except httpx.TimeoutException as exc:
            raise SourceQueryError("timeout", str(exc)) from exc
        except httpx.HTTPError as exc:
            raise SourceQueryError("transport_error", str(exc)) from exc

        if response.status_code >= 500:
            raise SourceQueryError("server_error", f"HTTP {response.status_code}")
        if response.status_code >= 400:
            raise SourceQueryError(
                "client_error",
                f"HTTP {response.status_code}: {response.text[:200]}",
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise SourceQueryError("invalid_response", str(exc)) from exc

        if payload.get("status") != "success":
            err = payload.get("error") or "unknown"
            raise SourceQueryError("query_error", str(err))

        data = payload.get("data") or {}
        result = data.get("result") or []
        truncated = False
        if max_series is not None and len(result) > max_series:
            result = result[:max_series]
            truncated = True

        series_list: list[QuerySeries] = []
        for entry in result:
            labels = dict(entry.get("metric") or {})
            samples = entry.get("values") or []
            if not samples:
                continue
            df = pd.DataFrame(
                [(float(ts), float(val)) for ts, val in samples],
                columns=["timestamp", "y"],
            )
            series_list.append(QuerySeries(labels=labels, samples=df))

        return QueryResult(series=series_list, truncated=truncated)


__all__ = [
    "PromQLSource",
    "QueryResult",
    "QuerySeries",
    "SourceQueryError",
]
