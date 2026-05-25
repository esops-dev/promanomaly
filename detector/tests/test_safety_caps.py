"""Tests for the safety cardinality caps and detect_duration_seconds gauge."""

from __future__ import annotations

import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline


def _multi_series(n: int) -> list[StubResponse]:
    series = [make_series({"instance": f"h{i}"}, clean_baseline(n=120, seed=i)) for i in range(n)]
    return series


def _cfg(*, max_series_per_query: int = 1000, max_total_series: int = 20000) -> Config:
    return Config.model_validate(
        {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://stub"},
            "safety": {
                "max_series_per_query": max_series_per_query,
                "max_total_series": max_total_series,
            },
            "defaults": {
                "window": "5m",
                "step": "15s",
                "min_points": 5,
                "warmup_policy": "emit_warming_up",
                "alert_thresholds": {"score": 3.0},
            },
            "groups": [
                {
                    "name": "ga",
                    "queries": [{"id": "ma", "promql": "up_a", "detectors": [{"name": "MAD"}]}],
                },
                {
                    "name": "gb",
                    "queries": [{"id": "mb", "promql": "up_b", "detectors": [{"name": "MAD"}]}],
                },
            ],
        }
    )


@pytest.mark.asyncio
async def test_max_total_series_cap_spans_groups(stub_source: StubSource) -> None:
    cfg = _cfg(max_series_per_query=10, max_total_series=5)

    series = _multi_series(6)
    stub_source.respond(lambda _: StubResponse(series=list(series)))

    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)

    # First group eats the full budget (and gets capped by max_series_per_query
    # to 6, then by max_total_series to 5).
    first = await runner.run_group("ga")
    assert first.succeeded
    assert first.series_count == 5

    # Second group sees the sticky 5 from ga in _last_series_count and
    # has zero remaining budget — none of its series are scored.
    second = await runner.run_group("gb")
    assert second.succeeded
    assert second.series_count == 0
    # Dropped counter bumped.
    dropped = ops.registry.get_sample_value(
        "anomaly_series_dropped_total",
        {"group": "gb", "reason": "cardinality_overflow"},
    )
    assert dropped == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_max_series_per_query_trims_within_a_query(stub_source: StubSource) -> None:
    # max_series_per_query is enforced inside the source client; the
    # stub source returns the full set unconditionally, so we instead
    # verify by setting an extreme cap and trusting the production
    # client honours its constructor argument. Here we only assert that
    # the runner forwards the cap to the source.
    cfg = _cfg(max_series_per_query=3, max_total_series=100)
    series = _multi_series(10)
    stub_source.respond(lambda _: StubResponse(series=series))

    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)
    await runner.run_group("ga")

    assert stub_source.queries[0]["max_series"] == 3


@pytest.mark.asyncio
async def test_detect_duration_seconds_gauge_populated(stub_source: StubSource) -> None:
    cfg = _cfg(max_series_per_query=100, max_total_series=100)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, clean_baseline(n=120))]))

    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)
    await runner.run_group("ga")

    elapsed = ops.registry.get_sample_value(
        "anomaly_detect_duration_seconds",
        {"group": "ga", "detector": "MAD"},
    )
    assert elapsed is not None
    assert elapsed >= 0.0
