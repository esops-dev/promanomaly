"""PromQL range-query client.

Thin wrapper around ``httpx`` that returns one ``QuerySeries`` per series
in the response, parsed into a pandas DataFrame ready for the detectors.

An optional :class:`TTLCache` collapses overlapping PromQL queries inside
a short TTL window — see ``cache.py`` for the rationale.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

import httpx
import numpy as np
import pandas as pd

from .cache import TTLCache
from .config import AuthConfig

# Shared connection pool sized for the typical 10k-series workload. The
# httpx default of 10 concurrent connections bottlenecks against a fast
# TSDB when stratified + discovery + main fetches storm in on the same
# tick. Tuned for one detector pod talking to one TSDB endpoint.
_HTTP_LIMITS = httpx.Limits(
    max_keepalive_connections=20,
    max_connections=50,
    keepalive_expiry=60.0,
)


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
    # Pre-computed in ``__post_init__`` because the runner reads
    # ``label_key`` 10+ times per series per refresh; recomputing the
    # sorted tuple on each property access was a measurable per-refresh
    # cost at 10k series.
    _cached_label_key: tuple[tuple[str, str], ...] = field(
        init=False, repr=False, compare=False, default=()
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "_cached_label_key", tuple(sorted(self.labels.items())))

    @property
    def label_key(self) -> tuple[tuple[str, str], ...]:
        """Hashable, order-stable identity of the series label set."""
        return self._cached_label_key


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
        cache: TTLCache[tuple[str, float, float, int, int], QueryResult] | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._auth = auth or AuthConfig()
        self._client: httpx.AsyncClient | None = None
        self._cache = cache

    @property
    def cache(self) -> TTLCache[tuple[str, float, float, int, int], QueryResult] | None:
        return self._cache

    def replace_cache(
        self,
        cache: TTLCache[tuple[str, float, float, int, int], QueryResult] | None,
    ) -> None:
        """Swap the cache out (used by config reload to honour new sizing)."""
        self._cache = cache

    async def __aenter__(self) -> PromQLSource:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def start(self) -> None:
        if self._client is not None:
            return
        kwargs: dict[str, Any] = {
            "timeout": self._timeout,
            "base_url": self._base_url,
            "limits": _HTTP_LIMITS,
        }
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

        When a :class:`TTLCache` is wired in, the response is served from
        cache when an identical ``(promql, window, step, max_series,
        end_bucketed)`` request has been seen inside the TTL. ``end`` is
        quantised to ``step_seconds`` so overlapping rolling windows
        coalesce — the seconds-of-skew between two callers within the
        same scrape bucket never causes a redundant TSDB hit.
        """
        cache_key: tuple[str, float, float, int, int] | None = None
        if self._cache is not None:
            # Quantise to step seconds so two callers requesting the same
            # promql/window within one step boundary share a cache slot
            # — the TSDB would return the same samples anyway. Use the
            # raw step so sub-second cadences (e.g. step=250ms) still get
            # one-bucket-per-step granularity; the small epsilon only
            # exists to keep an accidentally-zero step from raising
            # ZeroDivisionError.
            step = float(step_seconds) if step_seconds > 0 else 1e-6
            bucket = int(end // step)
            cache_key = (
                promql,
                float(window_seconds),
                float(step_seconds),
                int(max_series or 0),
                bucket,
            )
            cached = await self._cache_get(cache_key)
            if cached is not None:
                return cached

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
        # One transparent retry on a transient transport error. The common
        # case is a keepalive connection the TSDB (or an intermediary)
        # closed while idle between scrapes: the next request on that pooled
        # connection fails immediately and a fresh connection succeeds. A
        # single retry absorbs that blip instead of degrading the whole
        # group to serve_stale and tripping AnomalySourceFailing on a
        # harmless hiccup. Timeouts are NOT retried — under a slow TSDB a
        # retry only compounds load, and the per-query timeout already
        # bounds the wait. HTTP error envelopes (4xx/5xx) are real server
        # responses handled below, not transport failures, so they never
        # reach this retry.
        response: httpx.Response | None = None
        transport_exc: httpx.HTTPError | None = None
        for _attempt in range(2):
            try:
                response = await self._client.get("/api/v1/query_range", params=params)
                break
            except httpx.TimeoutException as exc:
                raise SourceQueryError("timeout", _exc_detail(exc)) from exc
            except httpx.HTTPError as exc:
                transport_exc = exc
        if response is None:
            assert transport_exc is not None
            raise SourceQueryError("transport_error", _exc_detail(transport_exc)) from transport_exc

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
            df = _build_samples_frame(samples)
            if df.empty:
                continue
            series_list.append(QuerySeries(labels=labels, samples=df))

        query_result = QueryResult(series=series_list, truncated=truncated)
        if self._cache is not None and cache_key is not None:
            await self._cache_put(cache_key, query_result)
        return query_result

    async def _cache_get(
        self, cache_key: tuple[str, float, float, int, int]
    ) -> QueryResult | None:
        """Read from the cache without blocking the event loop.

        The in-process :class:`TTLCache` is a lock-guarded memory lookup and
        runs inline; the Redis backend (``is_blocking_io=True``) does socket
        I/O, so it is offloaded to a worker thread to keep the loop free for
        other groups' detection runs.
        """
        assert self._cache is not None
        if getattr(self._cache, "is_blocking_io", False):
            return await asyncio.to_thread(self._cache.get, cache_key, time.monotonic())
        return self._cache.get(cache_key, time.monotonic())

    async def _cache_put(
        self, cache_key: tuple[str, float, float, int, int], value: QueryResult
    ) -> None:
        """Write to the cache without blocking the event loop (see ``_cache_get``)."""
        assert self._cache is not None
        if getattr(self._cache, "is_blocking_io", False):
            await asyncio.to_thread(self._cache.put, cache_key, value, time.monotonic())
            return
        self._cache.put(cache_key, value, time.monotonic())


def _exc_detail(exc: Exception) -> str:
    """Human-readable detail for a transport exception.

    httpx transport errors (``RemoteProtocolError`` for a server-closed
    connection, bare ``ConnectError``/``ReadError``) frequently stringify to
    an empty message, which left the ``source_query_failed`` log line and the
    ``anomaly_source_failure_total`` counter with no indication of *what*
    failed. Fall back to the exception class name so the failure is always
    diagnosable.
    """
    return str(exc) or type(exc).__name__


def _build_samples_frame(samples: list[list[Any]]) -> pd.DataFrame:
    """Vectorised parse of Prometheus ``[[ts, "value"], ...]`` payloads.

    Prometheus returns sample values as strings (including ``"NaN"`` /
    ``"+Inf"`` / ``"-Inf"``); detectors downstream assume finite floats,
    so non-finite samples are dropped here rather than propagating into
    MAD/Hampel medians as silent NaN poison. Vectorising via numpy
    sidesteps the per-cell Python ``float()`` cost — at 7d/15s windows
    over hundreds of series it was the dominant per-fetch CPU.
    """
    n = len(samples)
    ts_arr = np.empty(n, dtype=np.float64)
    y_arr = np.empty(n, dtype=np.float64)
    for i, (ts, val) in enumerate(samples):
        # Prometheus's HTTP API returns ts as a JSON number, but some
        # variants (VictoriaMetrics error envelopes, Mimir downstream
        # bridges) have historically returned it as a string. Coerce
        # via the same NaN-fallback path as values so a malformed
        # sample is dropped by the finite mask rather than crashing.
        try:
            ts_arr[i] = float(ts)
        except (TypeError, ValueError):
            ts_arr[i] = np.nan
        try:
            y_arr[i] = float(val)
        except (TypeError, ValueError):
            y_arr[i] = np.nan
    finite = np.isfinite(y_arr) & np.isfinite(ts_arr)
    if not finite.all():
        ts_arr = ts_arr[finite]
        y_arr = y_arr[finite]
    return pd.DataFrame({"timestamp": ts_arr, "y": y_arr})


__all__ = [
    "PromQLSource",
    "QueryResult",
    "QuerySeries",
    "SourceQueryError",
]
