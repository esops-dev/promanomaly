"""Tests for anomaly recurrence pattern detection."""

from __future__ import annotations

import datetime
from typing import Any

import pandas as pd
import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.exporter import OperationalMetrics
from promanomaly.recurrence import compute_recurrence_samples
from promanomaly.runner import Runner
from promanomaly.source import QuerySeries
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource


def _outside_series(
    firing_timestamps: list[float],
    clear_timestamps: list[float],
    labels: dict[str, str],
) -> QuerySeries:
    """Build an anomaly_outside_threshold series with known firing times."""
    timestamps = sorted(firing_timestamps + clear_timestamps)
    values = [1.0 if t in firing_timestamps else 0.0 for t in timestamps]
    df = pd.DataFrame({"timestamp": timestamps, "y": values})
    return QuerySeries(labels=labels, samples=df)


def _ts(year: int, month: int, day: int, hour: int) -> float:
    """UTC timestamp for a given date+hour."""
    return datetime.datetime(year, month, day, hour, tzinfo=datetime.UTC).timestamp()


def test_recurrence_basic_buckets() -> None:
    """Firing on Tuesday at 10:00 UTC produces a non-zero score for (Tue, 10)."""
    # Two Tuesdays: one fires at 10:00, the other is clear at 10:00.
    labels = {"id": "errors", "group": "g1"}
    tue1_10 = _ts(2025, 5, 6, 10)  # Tuesday
    tue2_10 = _ts(2025, 5, 13, 10)  # next Tuesday

    series = _outside_series(
        firing_timestamps=[tue1_10],
        clear_timestamps=[tue2_10],
        labels=labels,
    )

    samples = compute_recurrence_samples([series], group="g1")

    # Find the Tue/10 bucket.
    tue_10 = [
        s
        for s in samples
        if dict(s.labels).get("day_of_week") == "Tue" and dict(s.labels).get("hour_of_day") == "10"
    ]
    assert len(tue_10) == 1
    assert tue_10[0].value == 0.5  # 1 firing out of 2 samples


def test_recurrence_empty_series_emits_nothing() -> None:
    samples = compute_recurrence_samples([], group="g1")
    assert samples == []


def test_recurrence_all_clear_emits_zero_scores() -> None:
    labels = {"id": "errors", "group": "g1"}
    mon_9 = _ts(2025, 5, 5, 9)  # Monday
    mon_10 = _ts(2025, 5, 5, 10)

    series = _outside_series(
        firing_timestamps=[],
        clear_timestamps=[mon_9, mon_10],
        labels=labels,
    )
    samples = compute_recurrence_samples([series], group="g1")
    assert all(s.value == 0.0 for s in samples)


def test_recurrence_filters_by_group() -> None:
    labels = {"id": "errors", "group": "other"}
    mon_9 = _ts(2025, 5, 5, 9)

    series = _outside_series(
        firing_timestamps=[mon_9],
        clear_timestamps=[],
        labels=labels,
    )
    samples = compute_recurrence_samples([series], group="g1")
    assert samples == []


def test_recurrence_multiple_ids() -> None:
    labels_a = {"id": "a", "group": "g"}
    labels_b = {"id": "b", "group": "g"}
    mon_9 = _ts(2025, 5, 5, 9)

    series_a = _outside_series(firing_timestamps=[mon_9], clear_timestamps=[], labels=labels_a)
    series_b = _outside_series(firing_timestamps=[], clear_timestamps=[mon_9], labels=labels_b)

    samples = compute_recurrence_samples([series_a, series_b], group="g")

    ids = {dict(s.labels)["id"] for s in samples}
    assert "a" in ids
    assert "b" in ids

    a_samples = [s for s in samples if dict(s.labels)["id"] == "a"]
    assert all(s.value == 1.0 for s in a_samples)
    b_samples = [s for s in samples if dict(s.labels)["id"] == "b"]
    assert all(s.value == 0.0 for s in b_samples)


def test_recurrence_deduplicates_across_detectors() -> None:
    """Multiple detectors scoring the same id should not inflate the count."""
    labels_mad = {"id": "errors", "group": "g1", "detector": "MAD"}
    labels_hampel = {"id": "errors", "group": "g1", "detector": "Hampel"}
    mon_9 = _ts(2025, 5, 5, 9)

    # Both detectors fire at the same timestamp.
    series_mad = _outside_series(firing_timestamps=[mon_9], clear_timestamps=[], labels=labels_mad)
    series_hampel = _outside_series(
        firing_timestamps=[mon_9], clear_timestamps=[], labels=labels_hampel
    )

    samples = compute_recurrence_samples([series_mad, series_hampel], group="g1")

    mon_9_buckets = [
        s
        for s in samples
        if dict(s.labels).get("day_of_week") == "Mon" and dict(s.labels).get("hour_of_day") == "9"
    ]
    assert len(mon_9_buckets) == 1
    # One observation, one firing -> rate 1.0, not inflated to 2/2.
    assert mon_9_buckets[0].value == 1.0


def test_recurrence_dedup_any_detector_fires() -> None:
    """If one detector fires and another doesn't, the point counts as firing."""
    labels_mad = {"id": "errors", "group": "g1", "detector": "MAD"}
    labels_hampel = {"id": "errors", "group": "g1", "detector": "Hampel"}
    mon_9 = _ts(2025, 5, 5, 9)

    series_mad = _outside_series(firing_timestamps=[mon_9], clear_timestamps=[], labels=labels_mad)
    series_hampel = _outside_series(
        firing_timestamps=[], clear_timestamps=[mon_9], labels=labels_hampel
    )

    samples = compute_recurrence_samples([series_mad, series_hampel], group="g1")

    mon_9_buckets = [
        s
        for s in samples
        if dict(s.labels).get("day_of_week") == "Mon" and dict(s.labels).get("hour_of_day") == "9"
    ]
    assert len(mon_9_buckets) == 1
    # max(1.0, 0.0) = 1.0 -> firing. One observation -> rate 1.0.
    assert mon_9_buckets[0].value == 1.0


def test_recurrence_labels_include_required_fields() -> None:
    labels = {"id": "errors", "group": "g1"}
    mon_9 = _ts(2025, 5, 5, 9)
    series = _outside_series(firing_timestamps=[mon_9], clear_timestamps=[], labels=labels)
    samples = compute_recurrence_samples([series], group="g1")
    assert len(samples) > 0
    for sample in samples:
        label_dict = dict(sample.labels)
        assert "id" in label_dict
        assert "group" in label_dict
        assert "day_of_week" in label_dict
        assert "hour_of_day" in label_dict
        assert sample.metric == "anomaly_recurrence_score"


@pytest.mark.asyncio
async def test_runner_emits_recurrence_when_enabled(stub_source: StubSource) -> None:
    """When recurrence.enabled is true, the runner emits recurrence scores."""
    mon_9 = _ts(2025, 5, 5, 9)

    def responder(promql: str) -> StubResponse:
        if "anomaly_outside_threshold" in promql:
            return StubResponse(
                series=[
                    _outside_series(
                        firing_timestamps=[mon_9],
                        clear_timestamps=[],
                        labels={"id": "q", "group": "g"},
                    )
                ]
            )
        n = 120
        ts = [float(i) for i in range(n)]
        ys = [5.0 + (10.0 if i == n - 1 else 0.0) for i in range(n)]
        df = pd.DataFrame({"timestamp": ts, "y": ys})
        return StubResponse(series=[QuerySeries(labels={"k": "v"}, samples=df)])

    stub_source.respond(responder)

    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {"window": "15m", "step": "1s", "min_points": 5},
        "groups": [
            {
                "name": "g",
                "recurrence": {"enabled": True, "lookback": "4w", "step": "1h"},
                "queries": [
                    {"id": "q", "promql": "up", "detectors": [{"name": "MAD"}]},
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    store = SnapshotStore()
    runner = Runner(cfg, stub_source, store, OperationalMetrics())
    result = await runner.run_group("g")

    assert result.succeeded
    recurrence = [s for s in result.samples if s.metric == "anomaly_recurrence_score"]
    assert len(recurrence) > 0
    # All emitted samples carry the required labels.
    for sample in recurrence:
        label_dict = dict(sample.labels)
        assert label_dict.get("group") == "g"
        assert "id" in label_dict
        assert "day_of_week" in label_dict
        assert "hour_of_day" in label_dict


@pytest.mark.asyncio
async def test_runner_skips_recurrence_when_disabled(stub_source: StubSource) -> None:
    n = 120
    ts = [float(i) for i in range(n)]
    ys = [5.0 + (10.0 if i == n - 1 else 0.0) for i in range(n)]
    df = pd.DataFrame({"timestamp": ts, "y": ys})
    stub_source.respond(lambda _: StubResponse(series=[QuerySeries(labels={"k": "v"}, samples=df)]))

    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {"window": "15m", "step": "1s", "min_points": 5},
        "groups": [
            {
                "name": "g",
                # recurrence not enabled (default)
                "queries": [
                    {"id": "q", "promql": "up", "detectors": [{"name": "MAD"}]},
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    store = SnapshotStore()
    runner = Runner(cfg, stub_source, store, OperationalMetrics())
    result = await runner.run_group("g")

    assert result.succeeded
    recurrence = [s for s in result.samples if s.metric == "anomaly_recurrence_score"]
    assert len(recurrence) == 0
