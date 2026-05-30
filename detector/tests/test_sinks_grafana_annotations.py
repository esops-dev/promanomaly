"""Tests for the Grafana annotations sink.

Covers 0->1 transition detection, change-point increment detection, the
cold-start baseline guard, rate-bounding, and failure isolation, mocked
with ``httpx.MockTransport``.
"""

from __future__ import annotations

import httpx
import pytest

from promanomaly.config import AuthConfig, GrafanaAnnotationsSinkConfig
from promanomaly.exporter import OperationalMetrics
from promanomaly.sinks.grafana_annotations import GrafanaAnnotationsSink
from promanomaly.state import GroupSnapshot, Sample


def _sink_with_handler(handler, **cfg_kwargs):  # type: ignore[no-untyped-def]
    ops = OperationalMetrics()
    config = GrafanaAnnotationsSinkConfig(url="http://grafana", **cfg_kwargs)
    sink = GrafanaAnnotationsSink(config, ops)
    sink._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://grafana"
    )
    return sink, ops


def _snapshot(outside: float, *, ts: float = 100.0, extra=()) -> GroupSnapshot:
    samples = [
        Sample(
            metric="anomaly_outside_threshold",
            labels=(("id", "errors"), ("group", "svc"), ("detector", "MAD")),
            value=outside,
        ),
        *extra,
    ]
    return GroupSnapshot(group="svc", timestamp=ts, samples=samples)


@pytest.mark.asyncio
async def test_zero_to_one_transition_posts_annotation() -> None:
    posts: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        posts.append(json.loads(request.content))
        return httpx.Response(200)

    sink, _ops = _sink_with_handler(handler)
    # First run establishes baseline (value 0) — no annotation.
    await sink.emit(_snapshot(0.0, ts=100.0))
    assert posts == []
    # Second run: 0 -> 1 transition annotates.
    await sink.emit(_snapshot(1.0, ts=160.0))
    assert len(posts) == 1
    ann = posts[0]
    assert ann["time"] == 160_000
    assert "id:errors" in ann["tags"]
    assert "group:svc" in ann["tags"]
    assert "detector:MAD" in ann["tags"]
    assert "promanomaly" in ann["tags"]


@pytest.mark.asyncio
async def test_already_firing_on_cold_start_does_not_annotate() -> None:
    posts: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append({})
        return httpx.Response(200)

    sink, _ops = _sink_with_handler(handler)
    # First-ever observation is value 1 -> baseline only, no replay storm.
    await sink.emit(_snapshot(1.0))
    assert posts == []
    # Staying at 1 also does not annotate.
    await sink.emit(_snapshot(1.0))
    assert posts == []


@pytest.mark.asyncio
async def test_change_point_increment_annotates() -> None:
    posts: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        posts.append(json.loads(request.content))
        return httpx.Response(200)

    sink, _ops = _sink_with_handler(handler)
    cp = lambda v: Sample(  # noqa: E731
        metric="anomaly_change_point_total",
        labels=(("id", "q"), ("group", "svc"), ("detector", "BOCPD")),
        value=v,
    )
    await sink.emit(GroupSnapshot(group="svc", timestamp=10.0, samples=[cp(3.0)]))
    assert posts == []  # baseline
    await sink.emit(GroupSnapshot(group="svc", timestamp=20.0, samples=[cp(4.0)]))
    assert len(posts) == 1
    assert "change-point" in posts[0]["text"]
    # Counter reset (restart) lowers the value — not an increment.
    await sink.emit(GroupSnapshot(group="svc", timestamp=30.0, samples=[cp(1.0)]))
    assert len(posts) == 1


@pytest.mark.asyncio
async def test_rate_bounding_drops_overflow_and_counts() -> None:
    posts: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append({})
        return httpx.Response(200)

    sink, ops = _sink_with_handler(handler, max_annotations_per_run=2)
    # Baseline run with three series at 0.
    base = [
        Sample(
            metric="anomaly_outside_threshold",
            labels=(("id", f"s{i}"), ("group", "svc"), ("detector", "MAD")),
            value=0.0,
        )
        for i in range(3)
    ]
    await sink.emit(GroupSnapshot(group="svc", timestamp=1.0, samples=base))
    # All three fire at once -> 3 events, budget 2 -> 1 dropped.
    firing = [
        Sample(
            metric="anomaly_outside_threshold",
            labels=(("id", f"s{i}"), ("group", "svc"), ("detector", "MAD")),
            value=1.0,
        )
        for i in range(3)
    ]
    await sink.emit(GroupSnapshot(group="svc", timestamp=2.0, samples=firing))
    assert len(posts) == 2
    assert _failure_total(ops, reason="rate_limited") == 1.0


@pytest.mark.asyncio
async def test_http_error_is_isolated_and_counted() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    sink, ops = _sink_with_handler(handler)
    await sink.emit(_snapshot(0.0))
    await sink.emit(_snapshot(1.0))  # transition -> post -> 500
    assert _failure_total(ops, reason="http_error") == 1.0


@pytest.mark.asyncio
async def test_state_is_pruned_per_group_to_bound_memory() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200)

    sink, _ops = _sink_with_handler(handler)

    def series(group: str, ident: str, value: float) -> Sample:
        return Sample(
            metric="anomaly_outside_threshold",
            labels=(("id", ident), ("group", group), ("detector", "MAD")),
            value=value,
        )

    # Group A churns through ephemeral ids; group B has one stable id.
    await sink.emit(GroupSnapshot(group="A", timestamp=1.0, samples=[series("A", "x0", 0.0)]))
    await sink.emit(GroupSnapshot(group="B", timestamp=1.0, samples=[series("B", "stable", 0.0)]))
    for i in range(1, 20):
        await sink.emit(
            GroupSnapshot(group="A", timestamp=float(i + 1), samples=[series("A", f"x{i}", 0.0)])
        )

    # Group A state is bounded to the latest run's series (not 20 ephemerals).
    a_keys = [k for k in sink._last_outside if dict(k).get("group") == "A"]
    assert len(a_keys) == 1
    # Group B's entry is untouched by group A's runs.
    b_keys = [k for k in sink._last_outside if dict(k).get("group") == "B"]
    assert len(b_keys) == 1


def test_auth_must_be_none_or_bearer() -> None:
    with pytest.raises(ValueError, match="token auth"):
        GrafanaAnnotationsSinkConfig(url="http://grafana", auth=AuthConfig(type="basic"))


@pytest.mark.asyncio
async def test_start_sets_bearer_header_from_token_file(tmp_path) -> None:  # type: ignore[no-untyped-def]
    # The Helm chart mounts the Grafana token as a file and wires
    # ``token_file``; the sink must resolve it (not just the inline token)
    # or annotations post unauthenticated.
    token_path = tmp_path / "token"
    token_path.write_text("grafana-sa-token\n")  # trailing newline trimmed
    config = GrafanaAnnotationsSinkConfig(
        url="http://grafana",
        auth=AuthConfig(type="bearer", token_file=str(token_path)),
    )
    sink = GrafanaAnnotationsSink(config, OperationalMetrics())
    await sink.start()
    try:
        assert sink._client is not None
        assert sink._client.headers["Authorization"] == "Bearer grafana-sa-token"
    finally:
        await sink.aclose()


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
