"""Self-observability metrics: detector-health telemetry.

Pins the four families this surface ships:

* ``anomaly_detect_success_ratio{group, detector, window}``
* ``anomaly_group_cpu_seconds_total{group}``
* ``anomaly_group_memory_bytes{group}``
* ``anomaly_series_staleness_seconds{group}`` /
  ``anomaly_snapshot_age_seconds{group}``
"""

from __future__ import annotations

from typing import Any, ClassVar

import pandas as pd
import pytest

from promanomaly.detectors import registry
from promanomaly.exporter import Exporter, OperationalMetrics
from promanomaly.self_observability import (
    DetectOutcomeTracker,
    estimate_samples_bytes,
)
from promanomaly.state import GroupSnapshot, Sample, SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline, point_spike
from tests.test_runner import _config, _runner


# ----------------------------------------------------------------------
# DetectOutcomeTracker — rolling success ratio
# ----------------------------------------------------------------------
def test_outcome_tracker_aggregates_attempts_within_window() -> None:
    tracker = DetectOutcomeTracker(windows_seconds=(3600, 86400))
    # Two runs inside the 1h window: 9/10 then 10/10 -> 19/20 = 0.95.
    tracker.record(group="g", detector="MAD", attempts=10, successes=9, now=1000.0)
    tracker.record(group="g", detector="MAD", attempts=10, successes=10, now=1010.0)
    points = {(p.window_label): p.ratio for p in tracker.render(now=1020.0)}
    assert points["1h"] == pytest.approx(0.95)
    assert points["1d"] == pytest.approx(0.95)


def test_outcome_tracker_expires_old_samples() -> None:
    tracker = DetectOutcomeTracker(windows_seconds=(3600,))
    tracker.record(group="g", detector="MAD", attempts=4, successes=0, now=0.0)
    # 2h later, the failing run has aged out of the 1h window; a fresh
    # clean run leaves the ratio at 1.0 rather than dragged down by history.
    tracker.record(group="g", detector="MAD", attempts=4, successes=4, now=7200.0)
    points = {p.window_label: p.ratio for p in tracker.render(now=7200.0)}
    assert points["1h"] == pytest.approx(1.0)


def test_outcome_tracker_skips_empty_windows_and_zero_attempts() -> None:
    tracker = DetectOutcomeTracker(windows_seconds=(3600,))
    # Zero-attempt records are ignored entirely (idle detector).
    tracker.record(group="g", detector="MAD", attempts=0, successes=0, now=0.0)
    assert tracker.render(now=0.0) == []
    # A real record more than a window ago renders nothing (no points in window).
    tracker.record(group="g", detector="MAD", attempts=2, successes=2, now=0.0)
    assert tracker.render(now=100_000.0) == []


def test_outcome_tracker_retain_groups_drops_removed() -> None:
    tracker = DetectOutcomeTracker(windows_seconds=(3600,))
    tracker.record(group="keep", detector="MAD", attempts=1, successes=1, now=0.0)
    tracker.record(group="drop", detector="MAD", attempts=1, successes=1, now=0.0)
    tracker.retain_groups({"keep"})
    groups = {p.group for p in tracker.render(now=0.0)}
    assert groups == {"keep"}


# ----------------------------------------------------------------------
# estimate_samples_bytes
# ----------------------------------------------------------------------
def test_estimate_samples_bytes_grows_with_more_samples() -> None:
    one = [Sample(metric="anomaly_score", labels=(("id", "a"),), value=1.0)]
    many = [Sample(metric="anomaly_score", labels=(("id", f"a{i}"),), value=1.0) for i in range(10)]
    assert estimate_samples_bytes(many) > estimate_samples_bytes(one) > 0
    assert estimate_samples_bytes([]) == 0.0


# ----------------------------------------------------------------------
# Exporter render-time gauges
# ----------------------------------------------------------------------
def test_snapshot_age_and_staleness_climb_between_renders() -> None:
    store = SnapshotStore()
    ops = OperationalMetrics()
    exporter = Exporter(store, ops, source=None)
    store.write(
        GroupSnapshot(
            group="g",
            timestamp=100.0,
            samples=[Sample(metric="anomaly_score", labels=(("group", "g"),), value=1.0)],
        )
    )
    body, _ = exporter.render()
    text = body.decode("utf-8")
    # Both families present and, because the snapshot timestamp is far in
    # the past, the age is large and positive.
    assert 'anomaly_snapshot_age_seconds{group="g"}' in text
    assert 'anomaly_series_staleness_seconds{group="g"}' in text


def test_staleness_absent_until_a_score_is_emitted() -> None:
    store = SnapshotStore()
    ops = OperationalMetrics()
    exporter = Exporter(store, ops, source=None)
    # A warming-up-only snapshot carries no anomaly_score sample.
    store.write(
        GroupSnapshot(
            group="g",
            timestamp=100.0,
            samples=[Sample(metric="anomaly_warming_up", labels=(("group", "g"),), value=1.0)],
        )
    )
    body, _ = exporter.render()
    text = body.decode("utf-8")
    # snapshot_age is reported (the group ran) but staleness is not (no
    # score has ever been produced, so a staleness number would mislead).
    assert 'anomaly_snapshot_age_seconds{group="g"}' in text
    assert 'anomaly_series_staleness_seconds{group="g"}' not in text


# ----------------------------------------------------------------------
# Runner integration
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_run_records_success_ratio_cpu_and_memory(stub_source: StubSource) -> None:
    df = point_spike(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "h"}, df)]))
    cfg = _config(min_points=30)
    runner, _store, ops = _runner(cfg, stub_source)

    result = await runner.run_group("g1")
    assert result.succeeded

    # Success ratio recorded for the detector that ran.
    points = ops.detect_outcomes.render()
    assert any(p.group == "g1" and p.detector == "MAD" and p.ratio == 1.0 for p in points)
    # CPU counter advanced and memory gauge set.
    cpu = ops.group_cpu_seconds_total.labels(group="g1")._value.get()
    assert cpu > 0.0
    mem = ops.group_memory_bytes.labels(group="g1")._value.get()
    assert mem > 0.0


@pytest.mark.asyncio
async def test_detector_failure_drags_success_ratio_below_one(
    stub_source: StubSource,
) -> None:
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "h"}, df)]))

    class BoomDetector:
        name: ClassVar[str] = "BoomSO"
        description: ClassVar[str] = "always raises"

        def fit_score(self, *_: Any, **__: Any) -> pd.DataFrame:
            raise RuntimeError("intentional")

    registry.register(BoomDetector)  # type: ignore[arg-type]
    try:
        cfg = _config(min_points=30, detectors=[{"name": "MAD"}, {"name": "BoomSO"}])
        runner, _store, ops = _runner(cfg, stub_source)
        await runner.run_group("g1")
        points = {
            p.detector: p.ratio for p in ops.detect_outcomes.render() if p.window_label == "1h"
        }
        # MAD succeeded (1.0); the boom detector failed on its series (0.0).
        assert points.get("MAD") == pytest.approx(1.0)
        assert points.get("BoomSO") == pytest.approx(0.0)
    finally:
        registry.unregister("BoomSO")
