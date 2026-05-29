"""Transient-transport-error handling in :class:`PromQLSource`.

A keepalive connection the TSDB closed while idle fails the next request on
that pooled connection and succeeds on a fresh one. The source retries such
transport errors exactly once so a harmless hiccup doesn't degrade the whole
group to serve_stale (and trip AnomalySourceFailing). Timeouts are not
retried. Transport errors that stringify empty still produce a diagnosable
detail (the exception class name).
"""

from __future__ import annotations

import httpx
import pytest

from promanomaly.source import PromQLSource, SourceQueryError

_OK_PAYLOAD = {
    "status": "success",
    "data": {
        "resultType": "matrix",
        "result": [{"metric": {"instance": "h"}, "values": [[0.0, "1.0"], [15.0, "2.0"]]}],
    },
}


def _source_with_handler(handler) -> PromQLSource:  # type: ignore[no-untyped-def]
    src = PromQLSource(base_url="http://stub", timeout=1.0)
    src._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://stub")
    return src


@pytest.mark.asyncio
async def test_transient_transport_error_is_retried_once() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            # Simulate a server-closed keepalive connection.
            raise httpx.RemoteProtocolError("Server disconnected", request=request)
        return httpx.Response(200, json=_OK_PAYLOAD)

    src = _source_with_handler(handler)
    try:
        result = await src.range_query(promql="up", end=100.0, window_seconds=60.0, step_seconds=15.0)
    finally:
        await src.close()

    assert calls["n"] == 2, "first attempt fails, retry succeeds"
    assert result.series and result.series[0].labels == {"instance": "h"}


@pytest.mark.asyncio
async def test_transport_error_gives_up_after_one_retry() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        # Empty-message error: detail must still be diagnosable.
        raise httpx.ConnectError("", request=request)

    src = _source_with_handler(handler)
    try:
        with pytest.raises(SourceQueryError) as excinfo:
            await src.range_query(promql="up", end=100.0, window_seconds=60.0, step_seconds=15.0)
    finally:
        await src.close()

    assert calls["n"] == 2, "exactly one retry, then give up"
    assert excinfo.value.reason == "transport_error"
    # Empty str(exc) falls back to the class name rather than an empty detail.
    assert excinfo.value.message == "ConnectError"


@pytest.mark.asyncio
async def test_timeout_is_not_retried() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ReadTimeout("timed out", request=request)

    src = _source_with_handler(handler)
    try:
        with pytest.raises(SourceQueryError) as excinfo:
            await src.range_query(promql="up", end=100.0, window_seconds=60.0, step_seconds=15.0)
    finally:
        await src.close()

    assert calls["n"] == 1, "timeouts must not be retried — a retry only compounds TSDB load"
    assert excinfo.value.reason == "timeout"
