"""Runner tests covering failure isolation, warm-up, and reload."""

from __future__ import annotations

import time
from typing import Any, ClassVar

import pandas as pd
import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.detectors import registry
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import (
    REASON_DETECTOR_EXCEPTION,
    REASON_QUERY_EXCEPTION,
    REASON_TIMEOUT,
    REASON_UNKNOWN_DETECTOR,
    Runner,
)
from promanomaly.source import SourceQueryError
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline, point_spike, short_window


def _config(
    *,
    warmup_policy: str = "emit_warming_up",
    min_points: int = 30,
    detectors: list[dict[str, Any]] | None = None,
    extra_query: dict[str, Any] | None = None,
) -> Config:
    detectors = detectors or [{"name": "MAD"}]
    queries: list[dict[str, Any]] = [
        {"id": "metric_a", "promql": "up{job='x'}", "detectors": detectors}
    ]
    if extra_query:
        queries.append(extra_query)
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": min_points,
            "warmup_policy": warmup_policy,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [{"name": "g1", "queries": queries}],
    }
    return Config.model_validate(raw)


def _runner(cfg: Config, stub: StubSource) -> tuple[Runner, SnapshotStore, OperationalMetrics]:
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub, store, ops)
    return runner, store, ops


@pytest.mark.asyncio
async def test_run_group_produces_score_samples(stub_source: StubSource) -> None:
    df = point_spike(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "host-a"}, df)]))

    cfg = _config(min_points=30)
    runner, store, _ops = _runner(cfg, stub_source)

    result = await runner.run_group("g1")

    assert result.succeeded
    assert result.series_count == 1
    metrics = {s.metric for s in result.samples}
    # The core score-family metrics are always present; confidence /
    # baseline-stability are emitted when calibration has run, which
    # the default config (emit_baseline_stability=True) triggers on
    # this series's first pass.
    assert {
        "anomaly_score",
        "anomaly_outside_threshold",
        "anomaly_baseline",
        "anomaly_duration_seconds",
    }.issubset(metrics)
    assert metrics <= {
        "anomaly_score",
        "anomaly_outside_threshold",
        "anomaly_baseline",
        "anomaly_duration_seconds",
        "anomaly_confidence_score",
        "anomaly_baseline_stability",
        # Per-series severity plus the bounded fleet-density rollup.
        "anomaly_severity",
        "anomaly_active_series",
        "anomaly_density",
        "anomaly_severity_density",
    }
    score_sample = next(s for s in result.samples if s.metric == "anomaly_score")
    assert score_sample.label_dict["id"] == "metric_a"
    assert score_sample.label_dict["group"] == "g1"
    assert score_sample.label_dict["detector"] == "MAD"
    assert score_sample.label_dict["instance"] == "host-a"
    assert float(score_sample.value) > 3.0
    # outside threshold should fire
    outside = next(s for s in result.samples if s.metric == "anomaly_outside_threshold")
    assert outside.value == 1.0
    assert store.group_ready("g1")


@pytest.mark.asyncio
async def test_warmup_emit_warming_up_default(stub_source: StubSource) -> None:
    df = short_window(n=5)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _config(min_points=30)
    runner, _store, _ops = _runner(cfg, stub_source)

    result = await runner.run_group("g1")
    metrics = [s.metric for s in result.samples]
    assert metrics == ["anomaly_warming_up"]


@pytest.mark.asyncio
async def test_warmup_suppress_emits_nothing(stub_source: StubSource) -> None:
    df = short_window(n=5)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _config(min_points=30, warmup_policy="suppress")
    runner, _store, _ops = _runner(cfg, stub_source)

    result = await runner.run_group("g1")
    assert result.samples == []


@pytest.mark.asyncio
async def test_warmup_emit_with_flag_emits_score_and_flag(stub_source: StubSource) -> None:
    df = short_window(n=5)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _config(min_points=30, warmup_policy="emit_with_flag")
    runner, _store, _ops = _runner(cfg, stub_source)

    result = await runner.run_group("g1")
    metrics = {s.metric for s in result.samples}
    assert metrics == {"anomaly_warming_up", "anomaly_score", "anomaly_outside_threshold"}
    # All three metrics share the same label set so joins on (id, group)
    # survive the warmup boundary.
    for sample in result.samples:
        assert dict(sample.labels)["warming_up"] == "true", sample


@pytest.mark.asyncio
async def test_source_failure_serves_stale(stub_source: StubSource) -> None:
    df = clean_baseline(n=120)
    succeed = StubResponse(series=[make_series({"instance": "h"}, df)])
    fail = StubResponse(error=SourceQueryError("timeout", "boom"))
    state = {"call": 0}

    def respond(_: str) -> StubResponse:
        state["call"] += 1
        return succeed if state["call"] == 1 else fail

    stub_source.respond(respond)

    cfg = _config(min_points=30)
    runner, store, ops = _runner(cfg, stub_source)

    first = await runner.run_group("g1")
    assert first.succeeded

    second = await runner.run_group("g1")
    assert not second.succeeded
    # last good snapshot still present
    snap = store.get("g1")
    assert snap is not None
    assert any(s.metric == "anomaly_score" for s in snap.samples)
    # source-failure counter bumped
    families = [m for m in ops.registry.collect() if m.name == "anomaly_source_failure"]
    assert families, "anomaly_source_failure_total should be registered"


@pytest.mark.asyncio
async def test_per_series_detector_exception_does_not_block_other_detectors(
    stub_source: StubSource,
) -> None:
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    class BoomDetector:
        name: ClassVar[str] = "Boom"
        description: ClassVar[str] = "always blows up"
        defaults: ClassVar[dict[str, Any]] = {}

        def fit_score(self, *_: Any, **__: Any) -> pd.DataFrame:
            raise RuntimeError("intentional")

    registry.register(BoomDetector)  # type: ignore[arg-type]
    try:
        cfg = _config(
            detectors=[{"name": "Boom"}, {"name": "MAD"}],
        )
        runner, _store, ops = _runner(cfg, stub_source)
        result = await runner.run_group("g1")
        assert result.succeeded
        detector_names = {dict(s.labels).get("detector") for s in result.samples}
        assert "MAD" in detector_names
        assert "Boom" not in detector_names
        assert _collect(
            ops,
            "anomaly_failures_total",
            labels={
                "detector": "Boom",
                "reason": REASON_DETECTOR_EXCEPTION,
                "group": "g1",
            },
        ) == pytest.approx(1.0)
    finally:
        registry.unregister("Boom")


@pytest.mark.asyncio
async def test_unknown_detector_records_failure_and_continues(
    stub_source: StubSource,
) -> None:
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _config(detectors=[{"name": "NotARealDetector"}])
    runner, _store, ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    assert result.succeeded  # group ran; only that one query failed
    assert _collect(
        ops,
        "anomaly_failures_total",
        labels={
            "detector": "NotARealDetector",
            "reason": REASON_UNKNOWN_DETECTOR,
            "group": "g1",
        },
    ) == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_per_series_timeout_emits_counter(stub_source: StubSource) -> None:
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    class SlowDetector:
        name: ClassVar[str] = "Slow"
        description: ClassVar[str] = "sleeps forever"
        defaults: ClassVar[dict[str, Any]] = {}

        def fit_score(self, *_: Any, **__: Any) -> pd.DataFrame:
            time.sleep(2.0)
            return pd.DataFrame()

    registry.register(SlowDetector)  # type: ignore[arg-type]
    try:
        raw = {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://stub"},
            "safety": {"detect_timeout": "0.1s"},
            "defaults": {
                "window": "15m",
                "step": "15s",
                "min_points": 30,
                "warmup_policy": "emit_warming_up",
                "alert_thresholds": {"score": 3.0},
            },
            "groups": [
                {
                    "name": "g1",
                    "queries": [{"id": "m", "promql": "up", "detectors": [{"name": "Slow"}]}],
                }
            ],
        }
        cfg = Config.model_validate(raw)
        runner, _store, ops = _runner(cfg, stub_source)
        result = await runner.run_group("g1")
        assert result.succeeded
        # No anomaly_score from Slow — only timeout counter.
        assert all(s.metric != "anomaly_score" for s in result.samples)
        assert _collect(
            ops,
            "anomaly_failures_total",
            labels={"detector": "Slow", "reason": REASON_TIMEOUT, "group": "g1"},
        ) == pytest.approx(1.0)
    finally:
        registry.unregister("Slow")


@pytest.mark.asyncio
async def test_failing_query_does_not_stop_other_queries(stub_source: StubSource) -> None:
    good = clean_baseline(n=120)

    def respond(promql: str) -> StubResponse:
        if "bad" in promql:
            return StubResponse(error=SourceQueryError("server_error", "HTTP 500"))
        return StubResponse(series=[make_series({"instance": "h"}, good)])

    stub_source.respond(respond)

    cfg = _config(
        min_points=30,
        extra_query={
            "id": "m_bad",
            "promql": "bad_query",
            "detectors": [{"name": "MAD"}],
        },
    )
    runner, store, ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    assert not result.succeeded
    # bad query bumps the source-failure counter even when the top-level
    # group is marked failed.
    assert _collect(
        ops,
        "anomaly_source_failure_total",
        labels={"group": "g1", "reason": "server_error"},
    ) == pytest.approx(1.0)
    # No partial snapshot was written for this run, so the store is empty.
    assert store.get("g1") is None
    # Operational failure reason recorded.
    assert _collect(
        ops,
        "anomaly_failures_total",
        labels={"reason": REASON_QUERY_EXCEPTION, "group": "g1", "detector": ""},
    ) == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_reload_drops_removed_group(stub_source: StubSource) -> None:
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _config(min_points=30)
    runner, store, _ops = _runner(cfg, stub_source)
    await runner.run_group("g1")
    assert store.get("g1") is not None

    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "groups": [
            {
                "name": "g2",
                "queries": [{"id": "x", "promql": "up", "detectors": [{"name": "MAD"}]}],
            }
        ],
    }
    runner.replace_config(Config.model_validate(raw))
    assert store.get("g1") is None


def _collect(
    ops: OperationalMetrics, name: str, labels: dict[str, str] | None = None
) -> float | None:
    """Look up a single sample by name + labels in the operational registry."""
    return ops.registry.get_sample_value(name, labels or {})
