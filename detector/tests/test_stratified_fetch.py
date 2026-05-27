"""Runner-level tests for the sliding-window fetch strategy.

Verifies that a stratified detector (one exposing both
``query_window_seconds`` and ``baseline_refresh_interval_seconds``)
triggers the runner's two-window fetch: a short window every refresh
plus a long baseline that's cached across runs and only re-pulled at
the slower cadence. Plain rolling detectors keep the per-run single
fetch behaviour.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner, _stitch_series
from promanomaly.source import QuerySeries
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series


def _config_with_detectors(detectors: list[dict[str, Any]]) -> Config:
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "1m",
            "min_points": 5,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g_strat",
                "queries": [
                    {
                        "id": "weekly_signal",
                        "promql": "rate(metric[5m])",
                        "detectors": detectors,
                    }
                ],
            }
        ],
    }
    return Config.model_validate(raw)


def _runner_pair(cfg: Config, stub: StubSource) -> tuple[Runner, SnapshotStore]:
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub, store, ops)
    return runner, store


def _diurnal_history(*, days: int = 30, step_seconds: float = 60.0) -> pd.DataFrame:
    """Multi-week diurnal history wide enough to exercise the sliding-window cache."""
    n = int(days * 86400 / step_seconds)
    base_ts = 1_700_000_000.0 - (1_700_000_000.0 % 86400.0)
    timestamps = base_ts + np.arange(n) * step_seconds
    seconds_in_day = timestamps % 86400.0
    values = 5.0 * np.sin(2.0 * np.pi * seconds_in_day / 86400.0)
    return pd.DataFrame({"timestamp": timestamps, "y": values})


def test_stitch_series_drops_overlap_in_favour_of_fresh_window() -> None:
    base_df = pd.DataFrame({"timestamp": [10.0, 20.0, 30.0, 40.0], "y": [1.0, 2.0, 3.0, 4.0]})
    fresh_df = pd.DataFrame({"timestamp": [30.0, 40.0, 50.0], "y": [3.5, 4.5, 5.5]})
    base = QuerySeries(labels={"k": "v"}, samples=base_df)
    fresh = QuerySeries(labels={"k": "v"}, samples=fresh_df)
    stitched = _stitch_series(base, fresh)
    assert list(stitched.samples["timestamp"]) == [10.0, 20.0, 30.0, 40.0, 50.0]
    # Overlap at t=30 / t=40 takes the fresh values, not the cached ones.
    assert list(stitched.samples["y"]) == [1.0, 2.0, 3.5, 4.5, 5.5]


def test_stitch_series_handles_missing_base() -> None:
    fresh_df = pd.DataFrame({"timestamp": [50.0], "y": [5.5]})
    fresh = QuerySeries(labels={"k": "v"}, samples=fresh_df)
    stitched = _stitch_series(None, fresh)
    assert stitched is fresh


@pytest.mark.asyncio
async def test_stratified_detector_drives_two_fetches_then_caches_baseline(
    stub_source: StubSource,
) -> None:
    # With a long history available, the runner's first run should pull
    # both the short window and the long baseline; the second run inside
    # the baseline_refresh_interval should pull only the short window
    # again (the cached baseline is reused).
    history = _diurnal_history(days=30, step_seconds=60.0)
    series = make_series({"instance": "h"}, history)
    stub_source.respond(lambda _: StubResponse(series=[series]))

    cfg = _config_with_detectors(
        [{"name": "DayOfWeekMAD", "params": {"baseline_refresh_interval": "1h"}}]
    )
    runner, _store = _runner_pair(cfg, stub_source)

    await runner.run_group("g_strat")
    # First run: short fetch + baseline fetch.
    windows = [round(q["window"], 0) for q in stub_source.queries]
    assert 900.0 in windows  # 15m default short window
    assert max(windows) > 86400.0  # multi-week baseline

    first_run_calls = len(stub_source.queries)

    await runner.run_group("g_strat")
    # Second run: baseline cached, only the short fetch should happen.
    delta = len(stub_source.queries) - first_run_calls
    assert delta == 1, "second run should only fetch the short window"
    assert round(stub_source.queries[-1]["window"], 0) == 900.0


@pytest.mark.asyncio
async def test_non_stratified_detector_keeps_single_fetch_behaviour(
    stub_source: StubSource,
) -> None:
    # MAD has neither query_window_seconds nor baseline_refresh_interval_seconds;
    # the runner must keep the historical single-fetch path so we don't
    # silently inflate TSDB load for non-stratified configs.
    history = _diurnal_history(days=2, step_seconds=60.0)
    series = make_series({"instance": "h"}, history)
    stub_source.respond(lambda _: StubResponse(series=[series]))

    cfg = _config_with_detectors([{"name": "MAD"}])
    runner, _store = _runner_pair(cfg, stub_source)

    await runner.run_group("g_strat")
    await runner.run_group("g_strat")

    # Exactly one range_query per run, both at the group default window.
    assert len(stub_source.queries) == 2
    assert all(round(q["window"], 0) == 900.0 for q in stub_source.queries)


@pytest.mark.asyncio
async def test_baseline_refreshed_when_interval_elapses(
    stub_source: StubSource,
) -> None:
    # If the cached baseline ages past ``baseline_refresh_interval``,
    # the next run must re-pull it. Drive the cache directly to simulate
    # ageing without sleeping.
    history = _diurnal_history(days=30, step_seconds=60.0)
    series = make_series({"instance": "h"}, history)
    stub_source.respond(lambda _: StubResponse(series=[series]))

    cfg = _config_with_detectors(
        [{"name": "DayOfWeekMAD", "params": {"baseline_refresh_interval": "1h"}}]
    )
    runner, _store = _runner_pair(cfg, stub_source)

    await runner.run_group("g_strat")
    first_calls = len(stub_source.queries)

    # Age the cache by 2h so the next run re-pulls the baseline.
    runner.age_stratified_cache_entry_for_testing(("g_strat", "weekly_signal"), 2 * 3600.0)

    await runner.run_group("g_strat")
    delta = len(stub_source.queries) - first_calls
    # Stale cache: both short fetch AND baseline fetch happen again.
    assert delta == 2


@pytest.mark.asyncio
async def test_replace_config_invalidates_baseline_on_promql_change(
    stub_source: StubSource,
) -> None:
    # A reload that keeps (group, query.id) but rewrites query.promql must
    # discard the cached baseline — otherwise the runner would stitch the
    # previous signal's history onto the new signal's short window and
    # silently produce nonsense scores.
    history = _diurnal_history(days=30, step_seconds=60.0)
    series = make_series({"instance": "h"}, history)
    stub_source.respond(lambda _: StubResponse(series=[series]))

    cfg = _config_with_detectors(
        [{"name": "DayOfWeekMAD", "params": {"baseline_refresh_interval": "1h"}}]
    )
    runner, _store = _runner_pair(cfg, stub_source)
    await runner.run_group("g_strat")

    cache_key = ("g_strat", "weekly_signal")
    assert cache_key in runner.stratified_cache_keys_for_testing()

    # Reload with the same id but a different PromQL.
    new_raw = cfg.model_dump()
    new_raw["groups"][0]["queries"][0]["promql"] = "rate(other_metric[5m])"
    runner.replace_config(Config.model_validate(new_raw))
    assert cache_key not in runner.stratified_cache_keys_for_testing()


@pytest.mark.asyncio
async def test_replace_config_drops_baseline_for_removed_query(
    stub_source: StubSource,
) -> None:
    history = _diurnal_history(days=30, step_seconds=60.0)
    series = make_series({"instance": "h"}, history)
    stub_source.respond(lambda _: StubResponse(series=[series]))

    cfg = _config_with_detectors(
        [{"name": "DayOfWeekMAD", "params": {"baseline_refresh_interval": "1h"}}]
    )
    runner, _store = _runner_pair(cfg, stub_source)
    await runner.run_group("g_strat")

    assert ("g_strat", "weekly_signal") in runner.stratified_cache_keys_for_testing()

    # Reload with the same group renamed → cache for the old key drops.
    new_raw = cfg.model_dump()
    new_raw["groups"] = [
        {
            "name": "g_other",
            "queries": [
                {
                    "id": "other_signal",
                    "promql": "rate(metric[5m])",
                    "detectors": [
                        {
                            "name": "DayOfWeekMAD",
                            "params": {"baseline_refresh_interval": "1h"},
                        }
                    ],
                }
            ],
        }
    ]
    runner.replace_config(Config.model_validate(new_raw))
    assert ("g_strat", "weekly_signal") not in runner.stratified_cache_keys_for_testing()
