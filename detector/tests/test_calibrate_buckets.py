"""Tests for the ``promanomaly calibrate-buckets`` CLI helper."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml
from click.testing import CliRunner

from promanomaly import cli as cli_module
from promanomaly.cli import _analyse_bucket_structure, _recommend_detector, cli
from promanomaly.config import CURRENT_API_VERSION
from promanomaly.source import QuerySeries
from tests.conftest import StubResponse, StubSource


def _stub_source_with(monkeypatch: pytest.MonkeyPatch, responder: Any) -> StubSource:
    stub = StubSource()
    stub.respond(responder)
    monkeypatch.setattr(cli_module, "PromQLSource", lambda *_a, **_kw: stub)
    return stub


def _diurnal_series(*, days: int = 14, step_seconds: float = 300.0) -> QuerySeries:
    n = int(days * 86400 / step_seconds)
    base_ts = 1_700_000_000.0 - (1_700_000_000.0 % 86400.0)
    timestamps = base_ts + np.arange(n) * step_seconds
    seconds_in_day = timestamps % 86400.0
    values = 5.0 * np.sin(2.0 * np.pi * seconds_in_day / 86400.0)
    df = pd.DataFrame({"timestamp": timestamps, "y": values})
    return QuerySeries(labels={"instance": "h"}, samples=df)


def _weekly_series(*, weeks: int = 4, step_seconds: float = 600.0) -> QuerySeries:
    n = int(weeks * 7 * 86400 / step_seconds)
    base_ts = 1_700_000_000.0 - (1_700_000_000.0 % (7 * 86400.0))
    timestamps = base_ts + np.arange(n) * step_seconds
    seconds_in_day = timestamps % 86400.0
    seconds_in_week = timestamps % (7 * 86400.0)
    # Daily structure scaled by a weekly envelope so the ACF surfaces
    # peaks at both 24h and 7d.
    daily = 5.0 * np.sin(2.0 * np.pi * seconds_in_day / 86400.0)
    weekly_envelope = 2.0 + np.cos(2.0 * np.pi * seconds_in_week / (7 * 86400.0))
    values = daily * weekly_envelope
    df = pd.DataFrame({"timestamp": timestamps, "y": values})
    return QuerySeries(labels={"instance": "h"}, samples=df)


def test_analyse_recommends_hour_of_day_for_pure_diurnal_signal() -> None:
    analysis = _analyse_bucket_structure(
        series_list=[_diurnal_series(days=7)],
        step_seconds=300.0,
        window_seconds=7 * 86400.0,
    )
    assert analysis["status"] == "ok"
    rec = analysis["recommended_detector"]
    assert rec is not None
    assert rec["name"] == "HourOfDayMAD"
    assert "HourOfDayMAD" in rec["config_snippet"]


def test_analyse_recommends_day_of_week_when_weekly_pattern_dominates() -> None:
    analysis = _analyse_bucket_structure(
        series_list=[_weekly_series(weeks=4)],
        step_seconds=600.0,
        window_seconds=4 * 7 * 86400.0,
    )
    assert analysis["status"] == "ok"
    rec = analysis["recommended_detector"]
    assert rec is not None
    assert rec["name"] == "DayOfWeekMAD"


def test_analyse_returns_no_recommendation_for_constant_signal() -> None:
    df = pd.DataFrame({"timestamp": np.arange(100) * 60.0, "y": np.full(100, 3.14)})
    analysis = _analyse_bucket_structure(
        series_list=[QuerySeries(labels={}, samples=df)],
        step_seconds=60.0,
        window_seconds=6000.0,
    )
    assert analysis["status"] == "constant_signal"
    assert analysis["recommended_detector"] is None


def test_recommend_detector_requires_window_to_cover_two_periods() -> None:
    # A single-week window cannot reliably detect a weekly pattern even
    # if a 7d peak shows up in the ACF — too few full cycles. The
    # recommendation should fall back to HourOfDayMAD.
    rec = _recommend_detector(
        dominant_period_seconds=7 * 86400.0,
        candidates=[(7 * 86400.0, 0.9), (86400.0, 0.8)],
        window_seconds=5 * 86400.0,
    )
    assert rec is not None
    assert rec["name"] == "HourOfDayMAD"


def test_calibrate_buckets_cli_prints_snippet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_source_with(monkeypatch, lambda _: StubResponse(series=[_diurnal_series()]))

    result = CliRunner().invoke(
        cli,
        [
            "calibrate-buckets",
            "--query",
            "rate(metric[5m])",
            "--from",
            "-14d",
            "--step",
            "5m",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "recommended detector" in result.output
    assert "HourOfDayMAD" in result.output


def test_calibrate_buckets_cli_json_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_source_with(monkeypatch, lambda _: StubResponse(series=[_diurnal_series()]))

    result = CliRunner().invoke(
        cli,
        [
            "calibrate-buckets",
            "--query",
            "rate(metric[5m])",
            "--from",
            "-14d",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["recommended_detector"]["name"] == "HourOfDayMAD"


def test_calibrate_buckets_cli_exits_on_no_series(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_source_with(monkeypatch, lambda _: StubResponse(series=[]))
    result = CliRunner().invoke(
        cli,
        [
            "calibrate-buckets",
            "--query",
            "rate(metric[5m])",
            "--from",
            "-7d",
        ],
    )
    assert result.exit_code != 0
    assert "no series" in result.output.lower()


def test_validate_probe_surfaces_stratified_query_cost(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``validate --probe`` against a stratified detector reports the
    baseline window + refresh cadence + projected per-day query count
    so operators can spot a runaway baseline_refresh_interval=1m before
    it lands in production."""
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"refresh_interval": "1m"},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "weekly",
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
        ],
    }
    path = tmp_path / "stratified.yaml"
    path.write_text(yaml.safe_dump(cfg))

    _stub_source_with(
        monkeypatch,
        lambda _: StubResponse(series=[_diurnal_series(days=1)]),
    )

    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--probe"])
    assert result.exit_code == 0, result.output
    lines = [line for line in result.output.strip().splitlines() if line.strip()]
    per_query = json.loads(lines[0])
    assert "stratified" in per_query
    stratified = per_query["stratified"]
    assert stratified["baseline_window_seconds"] == pytest.approx(4 * 7 * 86400.0)
    assert stratified["baseline_refresh_seconds"] == pytest.approx(3600.0)
    assert stratified["baseline_queries_per_day"] == pytest.approx(24.0)
    assert stratified["short_queries_per_day"] == pytest.approx(1440.0)
    names = {d["name"] for d in stratified["detectors"]}
    assert "DayOfWeekMAD" in names
