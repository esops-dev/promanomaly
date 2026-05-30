"""Prometheus remote-write sink.

Serialises a group snapshot into a Prometheus ``WriteRequest`` (protobuf,
snappy-compressed) and POSTs it to a remote-write endpoint after each
successful run. The recommended path for long-horizon ``anomaly_density``
history that outlives scrape retention.

The protobuf ``WriteRequest`` schema is small and stable, so it is hand
encoded here (pure-Python wire format) rather than pulling in ``protobuf``
+ ``protoc``. Snappy block compression — mandatory per the remote-write
1.0 spec — is provided by ``cramjam`` (a wheel-distributed Rust binding,
no system libsnappy), imported lazily so the dependency is only required
when this sink is actually configured.
"""

from __future__ import annotations

import asyncio
import struct
from typing import Any

import httpx
import structlog

from ..config import RemoteWriteSinkConfig
from ..exporter import OperationalMetrics
from ..httpauth import build_httpx_auth
from ..state import GroupSnapshot, Sample
from .base import (
    REASON_CONNECT,
    REASON_HTTP,
    REASON_SERIALIZE,
    REASON_TIMEOUT,
    Sink,
)

logger = structlog.get_logger(__name__)

# Remote-write 1.0 request headers (Content-Encoding/Type fixed by spec).
_HEADERS = {
    "Content-Type": "application/x-protobuf",
    "Content-Encoding": "snappy",
    "X-Prometheus-Remote-Write-Version": "0.1.0",
    "User-Agent": "promanomaly",
}


def _varint(value: int) -> bytes:
    """Encode a non-negative integer as a protobuf base-128 varint."""
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _tag(field: int, wire_type: int) -> bytes:
    return _varint((field << 3) | wire_type)


def _len_delim(field: int, payload: bytes) -> bytes:
    return _tag(field, 2) + _varint(len(payload)) + payload


def _string_field(field: int, value: str) -> bytes:
    return _len_delim(field, value.encode("utf-8"))


def _double_field(field: int, value: float) -> bytes:
    # wire type 1 (fixed64), little-endian IEEE-754 double.
    return _tag(field, 1) + struct.pack("<d", value)


def _int64_field(field: int, value: int) -> bytes:
    # protobuf int64 uses a plain varint; remote-write timestamps are
    # positive milliseconds so the negative-value 10-byte case never hits.
    return _tag(field, 0) + _varint(value)


def _encode_label(name: str, value: str) -> bytes:
    return _string_field(1, name) + _string_field(2, value)


def _encode_sample(value: float, timestamp_ms: int) -> bytes:
    return _double_field(1, value) + _int64_field(2, timestamp_ms)


def _encode_timeseries(labels: list[tuple[str, str]], value: float, timestamp_ms: int) -> bytes:
    body = b"".join(_len_delim(1, _encode_label(name, val)) for name, val in labels)
    body += _len_delim(2, _encode_sample(value, timestamp_ms))
    return body


def encode_write_request(samples: list[Sample], timestamp_ms: int) -> bytes:
    """Encode samples into a snappy-compressed Prometheus ``WriteRequest``.

    Each :class:`Sample` becomes one ``TimeSeries`` with a single point.
    The metric name is carried in the reserved ``__name__`` label as the
    remote-write protocol requires. The remote-write spec requires every
    series' labels sorted by name, so the *full* set (including
    ``__name__``) is sorted together — prepending ``__name__`` and sorting
    only the rest would emit out-of-order labels for any upper-case label
    name (``_`` sorts after upper-case ASCII), which strict receivers
    reject.
    """
    import cramjam  # lazy: only needed when this sink is configured

    body = bytearray()
    for sample in samples:
        labels: list[tuple[str, str]] = sorted([("__name__", sample.metric), *sample.labels])
        body += _len_delim(1, _encode_timeseries(labels, sample.value, timestamp_ms))
    return bytes(cramjam.snappy.compress_raw(bytes(body)))


class RemoteWriteSink(Sink):
    """POSTs each successful group snapshot to a remote-write endpoint."""

    name = "remote_write"

    def __init__(self, config: RemoteWriteSinkConfig, operational: OperationalMetrics) -> None:
        super().__init__(operational)
        self._config = config
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        if self._client is not None:
            return
        kwargs: dict[str, Any] = {"timeout": self._config.timeout_seconds}
        kwargs.update(build_httpx_auth(self._config.auth))
        self._client = httpx.AsyncClient(**kwargs)

    async def emit(self, snapshot: GroupSnapshot) -> None:
        if not snapshot.samples:
            return
        if self._client is None:
            await self.start()
        assert self._client is not None
        timestamp_ms = int(snapshot.timestamp * 1000)
        try:
            # Protobuf encoding is pure-Python and snappy compression is
            # CPU-bound; on a 10k-series snapshot this is tens of
            # milliseconds. Run it in a worker thread so a concurrent
            # /metrics scrape on the event loop isn't blocked by the encode.
            payload = await asyncio.to_thread(encode_write_request, snapshot.samples, timestamp_ms)
        except Exception as exc:  # serialization / missing cramjam
            logger.error("remote_write_serialize_failed", group=snapshot.group, error=str(exc))
            self._count_failure(REASON_SERIALIZE)
            return
        try:
            response = await self._client.post(self._config.url, content=payload, headers=_HEADERS)
            response.raise_for_status()
        except httpx.TimeoutException:
            logger.warning("remote_write_timeout", group=snapshot.group, url=self._config.url)
            self._count_failure(REASON_TIMEOUT)
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "remote_write_http_error",
                group=snapshot.group,
                status=exc.response.status_code,
            )
            self._count_failure(REASON_HTTP)
        except httpx.HTTPError as exc:
            logger.warning("remote_write_connect_error", group=snapshot.group, error=str(exc))
            self._count_failure(REASON_CONNECT)

    async def aclose(self) -> None:
        if self._client is None:
            return
        try:
            await self._client.aclose()
        finally:
            self._client = None


__all__ = ["RemoteWriteSink", "encode_write_request"]
