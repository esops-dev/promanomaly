"""Tests for the fleet anomaly-density rollup and severity/type emission."""

from __future__ import annotations

from typing import Any

import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline, point_spike


def _config(**defaults: Any) -> Config:
    base_defaults: dict[str, Any] = {
        "window": "15m",
        "step": "15s",
        "min_points": 30,
        "alert_thresholds": {"score": 3.0},
    }
    base_defaults.update(defaults)
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": base_defaults,
        "groups": [
            {
                "name": "g1",
                "queries": [{"id": "m", "promql": "up", "detectors": [{"name": "MAD"}]}],
            }
        ],
    }
    return Config.model_validate(raw)


def _runner(cfg: Config, stub: StubSource) -> tuple[Runner, SnapshotStore, OperationalMetrics]:
    store = SnapshotStore()
    ops = OperationalMetrics()
    return Runner(cfg, stub, store, ops), store, ops


def _by_metric(samples: list[Any], metric: str) -> list[Any]:
    return [s for s in samples if s.metric == metric]


@pytest.mark.asyncio
async def test_density_emitted_with_active_series(stub_source: StubSource) -> None:
    # Two series: one anomalous (spike), one clean -> density 0.5.
    def respond(_: str) -> StubResponse:
        return StubResponse(
            series=[
                make_series({"instance": "bad"}, point_spike(n=120)),
                make_series({"instance": "good"}, clean_baseline(n=120)),
            ]
        )

    stub_source.respond(respond)
    cfg = _config()
    runner, _store, _ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")

    active = _by_metric(result.samples, "anomaly_active_series")
    density = _by_metric(result.samples, "anomaly_density")
    assert len(active) == 1
    assert active[0].value == 1.0
    # Group-level density series (no detector / by labels) == 0.5.
    group_density = [
        s for s in density if "detector" not in dict(s.labels) and "by" not in dict(s.labels)
    ]
    assert len(group_density) == 1
    assert group_density[0].value == pytest.approx(0.5)
    # Per-detector partition present.
    det_density = [s for s in density if dict(s.labels).get("detector") == "MAD"]
    assert len(det_density) == 1
    assert det_density[0].value == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_severity_density_emitted(stub_source: StubSource) -> None:
    stub_source.respond(
        lambda _: StubResponse(series=[make_series({"instance": "bad"}, point_spike(n=120))])
    )
    cfg = _config()
    runner, _store, _ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    sev_density = _by_metric(result.samples, "anomaly_severity_density")
    assert len(sev_density) >= 1
    assert all(0.0 <= s.value <= 1.0 for s in sev_density)


@pytest.mark.asyncio
async def test_density_by_dimension(stub_source: StubSource) -> None:
    def respond(_: str) -> StubResponse:
        return StubResponse(
            series=[
                make_series({"namespace": "prod", "pod": "a"}, point_spike(n=120)),
                make_series({"namespace": "prod", "pod": "b"}, clean_baseline(n=120)),
                make_series({"namespace": "dev", "pod": "c"}, clean_baseline(n=120)),
            ]
        )

    stub_source.respond(respond)
    cfg = _config(density_by=["namespace"])
    runner, _store, _ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")

    slices = [
        s
        for s in _by_metric(result.samples, "anomaly_density")
        if dict(s.labels).get("by") == "namespace"
    ]
    by_ns = {dict(s.labels)["namespace"]: s.value for s in slices}
    assert by_ns["prod"] == pytest.approx(0.5)  # 1 of 2 prod pods firing
    assert by_ns["dev"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_no_density_when_all_warming_up(stub_source: StubSource) -> None:
    # min_points high so the short window never warms up -> no scored
    # series -> no density rollup.
    stub_source.respond(
        lambda _: StubResponse(series=[make_series({"instance": "h"}, clean_baseline(n=5))])
    )
    cfg = _config(min_points=100)
    runner, _store, _ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    assert _by_metric(result.samples, "anomaly_density") == []
    assert _by_metric(result.samples, "anomaly_active_series") == []


@pytest.mark.asyncio
async def test_severity_disabled(stub_source: StubSource) -> None:
    stub_source.respond(
        lambda _: StubResponse(series=[make_series({"instance": "h"}, point_spike(n=120))])
    )
    cfg = _config(emit_severity=False)
    runner, _store, _ops = _runner(cfg, stub_source)
    result = await runner.run_group("g1")
    assert _by_metric(result.samples, "anomaly_severity") == []
    # severity_density also suppressed when severity is off.
    assert _by_metric(result.samples, "anomaly_severity_density") == []
    # Density (count-based) still emitted.
    assert _by_metric(result.samples, "anomaly_density") != []


def test_density_by_high_cardinality_is_capped(stub_source: StubSource) -> None:
    # A density_by dimension that explodes past the per-dimension cap is
    # skipped (bounded-cardinality guard), while the group rollup and
    # per-detector partition still emit.
    from promanomaly.runner import _MAX_DENSITY_SLICES
    from promanomaly.state import Sample

    cfg = _config(density_by=["pod"])
    runner, _store, _ops = _runner(cfg, stub_source)
    group = cfg.groups[0]

    samples: list[Sample] = []
    for i in range(_MAX_DENSITY_SLICES + 5):
        labels = (("detector", "MAD"), ("group", "g1"), ("id", "m"), ("pod", f"p{i}"))
        samples.append(Sample("anomaly_outside_threshold", labels, 1.0 if i == 0 else 0.0))
        samples.append(Sample("anomaly_severity", labels, 0.5 if i == 0 else 0.0))

    out = runner._density_samples(group, samples)
    metrics = {s.metric for s in out}
    # Group rollup + per-detector partition still present.
    assert "anomaly_active_series" in metrics
    assert "anomaly_density" in metrics
    # No per-slice (by="pod") samples were emitted — the dimension was capped.
    slices = [s for s in out if dict(s.labels).get("by") == "pod"]
    assert slices == []


def test_density_counts_entity_once_across_cohort_and_per_host_detectors(
    stub_source: StubSource,
) -> None:
    # A host watched by BOTH a per-host detector (MAD) and a Cohort
    # detector is one entity for density purposes. The Cohort detector
    # stamps an extra ``cohort_label`` annotation that MAD does not; that
    # annotation must be stripped from the entity identity, or the host is
    # double-counted. Regression for that strip: here ``bad`` is normal on
    # its own (MAD clear) but diverges from the fleet (Cohort fires) — the
    # exact reason to run both detectors — so a wrong identity skews the
    # ratio, not just the absolute count.
    from promanomaly.state import Sample

    cfg = _config()
    runner, _store, _ops = _runner(cfg, stub_source)
    group = cfg.groups[0]

    def mad(instance: str, value: float) -> Sample:
        labels = (("detector", "MAD"), ("group", "g1"), ("id", "m"), ("instance", instance))
        return Sample("anomaly_outside_threshold", labels, value)

    def cohort(instance: str, value: float) -> Sample:
        labels = (
            ("cohort_label", "instance"),
            ("detector", "Cohort"),
            ("group", "g1"),
            ("id", "m"),
            ("instance", instance),
        )
        return Sample("anomaly_outside_threshold", labels, value)

    samples = [
        mad("bad", 0.0),
        cohort("bad", 1.0),  # diverges from the fleet only
        mad("good", 0.0),
        cohort("good", 0.0),
    ]

    out = runner._density_samples(group, samples)
    active = _by_metric(out, "anomaly_active_series")
    assert len(active) == 1
    assert active[0].value == 1.0  # one entity active, not two

    group_density = [
        s
        for s in _by_metric(out, "anomaly_density")
        if "detector" not in dict(s.labels) and "by" not in dict(s.labels)
    ]
    assert len(group_density) == 1
    # Two entities (bad, good), one active -> 0.5, not 1/4 from double-count.
    assert group_density[0].value == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_anomaly_type_opt_in(stub_source: StubSource) -> None:
    stub_source.respond(
        lambda _: StubResponse(series=[make_series({"instance": "h"}, point_spike(n=120))])
    )
    # Off by default.
    cfg_off = _config()
    runner, _store, _ops = _runner(cfg_off, stub_source)
    result = await runner.run_group("g1")
    assert _by_metric(result.samples, "anomaly_type_score") == []

    # On when configured -> emitted for the firing series with a type label.
    cfg_on = _config(emit_anomaly_type=True)
    runner2, _store2, _ops2 = _runner(cfg_on, stub_source)
    result2 = await runner2.run_group("g1")
    type_samples = _by_metric(result2.samples, "anomaly_type_score")
    assert len(type_samples) == 1
    assert dict(type_samples[0].labels)["type"] in {
        "point_spike",
        "step_up",
        "step_down",
        "level_drift",
        "variance_shift",
    }
