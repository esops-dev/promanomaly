"""Tests for the Prometheus remote-write sink.

Covers the hand-rolled protobuf ``WriteRequest`` encoding (round-tripped
through a minimal decoder), snappy framing, and the emit path's HTTP
behaviour + failure isolation, mocked with ``httpx.MockTransport``.
"""

from __future__ import annotations

import struct

import cramjam
import httpx
import pytest

from promanomaly.config import AuthConfig, RemoteWriteSinkConfig
from promanomaly.exporter import OperationalMetrics
from promanomaly.sinks.remote_write import RemoteWriteSink, encode_write_request
from promanomaly.state import GroupSnapshot, Sample


# --------------------------------------------------------------------------
# Minimal protobuf decoder (test-only) to verify the hand-rolled encoder.
# --------------------------------------------------------------------------
def _read_varint(buf: bytes, i: int) -> tuple[int, int]:
    shift = 0
    result = 0
    while True:
        byte = buf[i]
        i += 1
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result, i
        shift += 7


def _parse(buf: bytes) -> list[tuple[int, int, object]]:
    i = 0
    fields: list[tuple[int, int, object]] = []
    while i < len(buf):
        tag, i = _read_varint(buf, i)
        field, wire = tag >> 3, tag & 7
        if wire == 2:
            length, i = _read_varint(buf, i)
            val: object = buf[i : i + length]
            i += length
        elif wire == 0:
            val, i = _read_varint(buf, i)
        elif wire == 1:
            val = struct.unpack("<d", buf[i : i + 8])[0]
            i += 8
        else:  # pragma: no cover - unused wire types
            raise AssertionError(f"unexpected wire type {wire}")
        fields.append((field, wire, val))
    return fields


def _decode_write_request(payload: bytes) -> list[dict[str, object]]:
    raw = bytes(cramjam.snappy.decompress_raw(payload))
    series: list[dict[str, object]] = []
    for field, _wire, val in _parse(raw):
        assert field == 1
        ts_fields = _parse(val)  # type: ignore[arg-type]
        labels: dict[str, str] = {}
        sample: dict[str, float] = {}
        for f, _w, v in ts_fields:
            if f == 1:  # label
                pair = _parse(v)  # type: ignore[arg-type]
                name = (pair[0][2]).decode()  # type: ignore[union-attr]
                value = (pair[1][2]).decode()  # type: ignore[union-attr]
                labels[name] = value
            elif f == 2:  # sample
                sp = _parse(v)  # type: ignore[arg-type]
                sample = {"value": sp[0][2], "timestamp": sp[1][2]}  # type: ignore[dict-item]
        series.append({"labels": labels, "sample": sample})
    return series


def test_encode_write_request_roundtrips() -> None:
    samples = [
        Sample(metric="anomaly_score", labels=(("id", "cpu"), ("group", "fleet")), value=4.5),
        Sample(metric="anomaly_density", labels=(("group", "fleet"),), value=0.25),
    ]
    payload = encode_write_request(samples, timestamp_ms=1_700_000_000_000)
    decoded = _decode_write_request(payload)

    assert len(decoded) == 2
    first = decoded[0]
    # Metric name carried as __name__, labels sorted with __name__ first.
    assert first["labels"] == {"__name__": "anomaly_score", "id": "cpu", "group": "fleet"}
    assert first["sample"]["value"] == pytest.approx(4.5)
    assert first["sample"]["timestamp"] == 1_700_000_000_000


def _ordered_label_names(payload: bytes) -> list[str]:
    """Wire-order label names of the first series (order is lost by the dict
    decoder, but remote-write requires labels sorted by name)."""
    raw = bytes(cramjam.snappy.decompress_raw(payload))
    first_ts = _parse(raw)[0][2]
    names: list[str] = []
    for f, _w, v in _parse(first_ts):  # type: ignore[arg-type]
        if f == 1:
            names.append((_parse(v)[0][2]).decode())  # type: ignore[arg-type,union-attr]
    return names


def test_encode_write_request_sorts_labels_including_uppercase() -> None:
    # An upper-case label name sorts before ``__name__`` ('_' is 0x5F,
    # after upper-case ASCII). The full set must be sorted together or a
    # strict remote-write receiver rejects the out-of-order series.
    samples = [
        Sample(metric="anomaly_score", labels=(("Region", "eu"), ("id", "cpu")), value=1.0),
    ]
    payload = encode_write_request(samples, timestamp_ms=1)
    assert _ordered_label_names(payload) == ["Region", "__name__", "id"]


def _sink_with_handler(handler, **cfg_kwargs):  # type: ignore[no-untyped-def]
    ops = OperationalMetrics()
    config = RemoteWriteSinkConfig(url="http://stub/api/v1/write", **cfg_kwargs)
    sink = RemoteWriteSink(config, ops)
    sink._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://stub")
    return sink, ops


def _snapshot() -> GroupSnapshot:
    return GroupSnapshot(
        group="g1",
        timestamp=1_700_000_000.0,
        samples=[Sample(metric="anomaly_score", labels=(("id", "cpu"),), value=2.0)],
    )


@pytest.mark.asyncio
async def test_emit_posts_snappy_protobuf() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = dict(request.headers)
        captured["content"] = request.content
        return httpx.Response(200)

    sink, ops = _sink_with_handler(handler)
    await sink.emit(_snapshot())

    headers = captured["headers"]
    assert headers["content-encoding"] == "snappy"
    assert headers["content-type"] == "application/x-protobuf"
    assert headers["x-prometheus-remote-write-version"] == "0.1.0"
    decoded = _decode_write_request(captured["content"])  # type: ignore[arg-type]
    assert decoded[0]["labels"]["__name__"] == "anomaly_score"
    # No failures counted.
    assert _failure_total(ops) == 0.0


@pytest.mark.asyncio
async def test_emit_empty_snapshot_skips_post() -> None:
    called = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        called["n"] += 1
        return httpx.Response(200)

    sink, _ops = _sink_with_handler(handler)
    await sink.emit(GroupSnapshot(group="g1", timestamp=1.0, samples=[]))
    assert called["n"] == 0


@pytest.mark.asyncio
async def test_emit_http_error_is_isolated_and_counted() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    sink, ops = _sink_with_handler(handler)
    # Must not raise.
    await sink.emit(_snapshot())
    assert _failure_total(ops, reason="http_error") == 1.0


@pytest.mark.asyncio
async def test_emit_timeout_is_counted() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("slow")

    sink, ops = _sink_with_handler(handler)
    await sink.emit(_snapshot())
    assert _failure_total(ops, reason="timeout") == 1.0


def test_bearer_auth_sets_header() -> None:
    ops = OperationalMetrics()
    config = RemoteWriteSinkConfig(
        url="http://stub/api/v1/write",
        auth=AuthConfig(type="bearer", token="secret"),
    )
    RemoteWriteSink(config, ops)  # constructs without error
    # build_httpx_auth is exercised via start(); inspect the kwargs path.
    from promanomaly.httpauth import build_httpx_auth

    assert build_httpx_auth(config.auth)["headers"]["Authorization"] == "Bearer secret"


def _failure_total(ops: OperationalMetrics, reason: str | None = None) -> float:
    total = 0.0
    for metric in ops.registry.collect():
        if metric.name != "anomaly_sink_failures":
            continue
        for sample in metric.samples:
            if not sample.name.endswith("_total"):
                continue
            if reason is not None and sample.labels.get("reason") != reason:
                continue
            total += sample.value
    return total
