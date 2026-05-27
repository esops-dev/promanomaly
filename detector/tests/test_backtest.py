"""Tests for the ``promanomaly backtest`` real-history evaluator."""

from __future__ import annotations

from typing import Any

import pytest

from promanomaly.backtest import (
    BacktestReport,
    _build_injected_window,
    _parse_timestamp,
    _sliding_windows,
    render_markdown,
    run_backtest,
)
from promanomaly.config import CURRENT_API_VERSION, Config
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline


def _config(detectors: list[dict[str, Any]] | None = None) -> Config:
    detectors = detectors or [{"name": "MAD"}]
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {"id": "metric_a", "promql": "up{job='x'}", "detectors": detectors},
                ],
            }
        ],
    }
    return Config.model_validate(raw)


def test_sliding_windows_handles_short_series() -> None:
    df = clean_baseline(n=10)
    windows = _sliding_windows(df, window_samples=60, stride=15)
    assert len(windows) == 1
    assert len(windows[0]) == 10


def test_sliding_windows_carves_overlapping_chunks() -> None:
    df = clean_baseline(n=200)
    windows = _sliding_windows(df, window_samples=60, stride=15)
    # Each window is exactly window_samples long.
    assert all(len(w) == 60 for w in windows)
    # Overlap by stride: 200-60+1 = 141 starting indices, stride=15 → 10 windows.
    assert len(windows) == (200 - 60) // 15 + 1


def test_injection_preserves_timestamps_and_modifies_y() -> None:
    df = clean_baseline(n=60, mean=10.0, noise=0.05)
    injected = _build_injected_window(df, "point_spike")
    assert list(injected["timestamp"]) == list(df["timestamp"])
    # Last sample is the spike; should differ from the clean baseline.
    assert injected["y"].iloc[-1] != df["y"].iloc[-1]


@pytest.mark.asyncio
async def test_backtest_without_inject_reports_real_fire_rate(
    stub_source: StubSource,
) -> None:
    df = clean_baseline(n=240)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "host-a"}, df)]))
    cfg = _config()
    report = await run_backtest(config=cfg, source=stub_source, group_name="g1", inject=False)
    assert isinstance(report, BacktestReport)
    assert report.inject is False
    assert len(report.queries) == 1
    qreport = report.queries[0]
    assert qreport.query_id == "metric_a"
    assert qreport.series_count == 1
    assert len(qreport.detectors) == 1
    metrics = qreport.detectors[0]
    assert metrics.detector == "MAD"
    assert metrics.real_windows > 0
    # MAD on clean baseline should not fire.
    assert metrics.fires_on_real == 0
    assert metrics.real_fire_rate == 0.0


@pytest.mark.asyncio
async def test_backtest_with_inject_catches_synthetic_anomalies(
    stub_source: StubSource,
) -> None:
    df = clean_baseline(n=300, mean=10.0, noise=0.05)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "host-a"}, df)]))
    cfg = _config()
    report = await run_backtest(
        config=cfg,
        source=stub_source,
        group_name="g1",
        inject=True,
        max_windows=3,
    )
    metrics = report.queries[0].detectors[0]
    # We injected at least one anomaly per pattern per window — MAD
    # catches the point spike reliably.
    assert metrics.true_positive > 0
    # Magnitude breakdown is populated per pattern.
    assert set(metrics.magnitude_breakdown) == {"point_spike", "step_change", "level_drift"}
    point_spike = metrics.magnitude_breakdown["point_spike"]
    assert point_spike["true_positive"] >= 1


@pytest.mark.asyncio
async def test_backtest_unknown_group_raises(stub_source: StubSource) -> None:
    import click

    cfg = _config()
    with pytest.raises(click.ClickException):
        await run_backtest(config=cfg, source=stub_source, group_name="does-not-exist")


def test_render_markdown_inject_mode_includes_per_pattern_breakdown() -> None:
    report = BacktestReport(
        group="g1", inject=True, window_seconds=900.0, step_seconds=15.0, end=0.0
    )
    from promanomaly.backtest import DetectorMetrics, QueryReport

    qreport = QueryReport(query_id="metric_a", promql="up", series_count=1)
    metrics = DetectorMetrics(
        detector="MAD",
        true_positive=2,
        false_positive=0,
        false_negative=1,
        true_negative=4,
        fires_on_real=1,
        real_windows=4,
        magnitude_breakdown={
            "point_spike": {"true_positive": 1, "false_negative": 0},
            "step_change": {"true_positive": 1, "false_negative": 0},
            "level_drift": {"true_positive": 0, "false_negative": 1},
        },
    )
    qreport.detectors.append(metrics)
    report.queries.append(qreport)
    rendered = render_markdown(report)
    assert "# Backtest report" in rendered
    assert "MAD" in rendered
    assert "point_spike" in rendered
    assert "level_drift" in rendered
    # Inject mode prints precision/recall/F1.
    assert "Precision" in rendered


def test_parse_timestamp_handles_now_and_relative() -> None:
    now = 1_700_000_000.0
    assert _parse_timestamp("now", now=now) == now
    assert _parse_timestamp("-1h", now=now) == now - 3600.0
    assert _parse_timestamp("1700001234", now=now) == 1700001234.0
    # RFC3339 with explicit timezone.
    parsed = _parse_timestamp("2024-01-15T00:00:00Z", now=now)
    assert isinstance(parsed, float)


@pytest.mark.asyncio
async def test_backtest_honours_per_detector_threshold_override(
    stub_source: StubSource,
) -> None:
    # BOCPD's score is a posterior probability in [0, 1] and its
    # ``threshold`` param is typically 0.5 — far below the default
    # alert_thresholds.score of 3.0. Before the fix, backtest used the
    # bare group-level 3.0 threshold and BOCPD never "fired" in any
    # injected window, so its F1 collapsed to 0. Regression: BOCPD's
    # threshold override must flow through to the backtest fire check.
    df = clean_baseline(n=300, mean=10.0, noise=0.05)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "host-a"}, df)]))
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "metric_a",
                        "promql": "up",
                        "detectors": [
                            {"name": "BOCPD", "params": {"threshold": 0.5}},
                        ],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    report = await run_backtest(
        config=cfg, source=stub_source, group_name="g1", inject=True, max_windows=3
    )
    metrics = report.queries[0].detectors[0]
    # We don't pin specific TP/FP counts (BOCPD on injected step changes
    # is probabilistic); we pin the *existence* of injected signal —
    # without the threshold fix every injected detection was discarded
    # and tp+fn would equal 0 across every pattern.
    total_injected = sum(
        bucket.get("true_positive", 0) + bucket.get("false_negative", 0)
        for bucket in metrics.magnitude_breakdown.values()
    )
    assert total_injected > 0


@pytest.mark.asyncio
async def test_backtest_rejects_from_after_to(stub_source: StubSource) -> None:
    import click

    cfg = _config()
    with pytest.raises(click.ClickException, match="--from must be before --to"):
        await run_backtest(
            config=cfg,
            source=stub_source,
            group_name="g1",
            end=1_000_000.0,
            start=2_000_000.0,
        )


@pytest.mark.asyncio
async def test_backtest_accepts_from_to_range(stub_source: StubSource) -> None:
    df = clean_baseline(n=120)
    captured: dict[str, float] = {}

    def _respond(_promql: str) -> StubResponse:
        return StubResponse(series=[make_series({"instance": "host-a"}, df)])

    stub_source.respond(_respond)
    cfg = _config()
    # Explicit range — history_seconds should equal end-start, not the
    # 4x-window default.
    end_ts = 2_000_000_000.0
    start_ts = end_ts - 7200.0  # 2h window
    await run_backtest(
        config=cfg,
        source=stub_source,
        group_name="g1",
        inject=False,
        end=end_ts,
        start=start_ts,
    )
    # Last recorded query reflects the explicit range, not the default.
    last = stub_source.queries[-1]
    captured["window"] = last["window"]
    captured["end"] = last["end"]
    assert captured["window"] == 7200.0
    assert captured["end"] == end_ts


def test_render_markdown_real_mode_omits_pr_table() -> None:
    report = BacktestReport(
        group="g1", inject=False, window_seconds=900.0, step_seconds=15.0, end=0.0
    )
    from promanomaly.backtest import DetectorMetrics, QueryReport

    qreport = QueryReport(query_id="metric_a", promql="up", series_count=1)
    qreport.detectors.append(DetectorMetrics(detector="MAD", real_windows=10, fires_on_real=2))
    report.queries.append(qreport)
    rendered = render_markdown(report)
    assert "real fire-rate" in rendered.lower() or "fires on real history" in rendered.lower()
    assert "Precision" not in rendered
