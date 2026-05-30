"""Tests for the ``timeline`` CLI command."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from promanomaly.cli import cli
from promanomaly.cli._timeline import (
    METRIC_CHANGE_POINT,
    METRIC_OUTSIDE,
    METRIC_SEVERITY,
    run_timeline,
)
from promanomaly.source import QuerySeries


def _series(
    values: list[float],
    *,
    labels: dict[str, str] | None = None,
    start: float = 0.0,
    step: float = 60.0,
) -> QuerySeries:
    df = pd.DataFrame(
        {"timestamp": [start + float(i) * step for i in range(len(values))], "y": values}
    )
    return QuerySeries(labels=labels or {}, samples=df)


def _responder_for(series_by_metric: dict[str, list[QuerySeries]]) -> Any:
    """Return a responder branching on the metric name in the query."""

    def _responder(*, promql: str, end: float, window_seconds: float, step_seconds: float) -> Any:
        from tests.conftest import _QueryResult

        for metric, series in series_by_metric.items():
            if promql.startswith(metric):
                return _QueryResult(list(series))
        return _QueryResult([])

    return _responder


def test_timeline_text_output(cli_stub_source: Any, cli_runner: Any, monkeypatch: Any) -> None:
    series_by_metric = {
        METRIC_OUTSIDE: [
            _series(
                [0.0, 0.0, 1.0, 1.0],
                labels={"id": "errors", "group": "g1", "detector": "MAD"},
            )
        ],
        METRIC_CHANGE_POINT: [
            _series(
                [0.0, 0.0, 0.0, 1.0],
                labels={"id": "errors", "group": "g1", "detector": "CUSUM"},
            )
        ],
    }
    cli_stub_source(monkeypatch, _responder_for(series_by_metric))

    result = cli_runner.invoke(
        cli,
        ["timeline", "--from", "-1h", "--to", "now"],
    )

    assert result.exit_code == 0
    output = result.output
    assert "anomaly" in output
    assert "change_point" in output
    # The anomaly starts at t=120s and the change-point fires at t=180s, so the
    # anomaly line must appear before the change-point line.
    assert output.index("anomaly") < output.index("change_point")


def test_timeline_json_output(cli_stub_source: Any, cli_runner: Any, monkeypatch: Any) -> None:
    series_by_metric = {
        METRIC_OUTSIDE: [
            _series(
                [0.0, 1.0],
                labels={
                    "id": "errors",
                    "group": "g1",
                    "detector": "MAD",
                    "detector_instance": "short",
                },
            )
        ],
        METRIC_SEVERITY: [
            _series(
                [2.0, 4.5],
                labels={
                    "id": "errors",
                    "group": "g1",
                    "detector": "MAD",
                    "detector_instance": "short",
                },
            )
        ],
    }
    cli_stub_source(monkeypatch, _responder_for(series_by_metric))

    result = cli_runner.invoke(
        cli,
        ["timeline", "--from", "-1h", "--output", "json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert isinstance(payload, list)
    assert len(payload) == 1
    event = payload[0]
    assert event["kind"] == "anomaly"
    assert event["id"] == "errors"
    assert event["group"] == "g1"
    assert event["detector"] == "MAD"
    assert event["detector_instance"] == "short"
    assert event["severity"] == 4.5
    assert "time" in event
    assert "time_rfc3339" in event


def test_timeline_group_filter(cli_stub_source: Any, cli_runner: Any, monkeypatch: Any) -> None:
    series_by_metric = {
        METRIC_OUTSIDE: [
            _series([0.0, 1.0], labels={"id": "errors", "group": "g1", "detector": "MAD"})
        ],
    }
    stub = cli_stub_source(monkeypatch, _responder_for(series_by_metric))

    result = cli_runner.invoke(
        cli,
        ["timeline", "--from", "-1h", "--group", "g1", "--group", "g2"],
    )

    assert result.exit_code == 0
    # Every issued query carries the repeated-group label matcher.
    assert stub.calls
    for call in stub.calls:
        assert '{group=~"g1|g2"}' in call["promql"]


def test_timeline_source_error_exits(
    cli_stub_source: Any, cli_runner: Any, monkeypatch: Any
) -> None:
    def _responder(*, promql: str, end: float, window_seconds: float, step_seconds: float) -> Any:
        from promanomaly.source import SourceQueryError

        raise SourceQueryError("boom", "test error")

    cli_stub_source(monkeypatch, _responder)

    result = cli_runner.invoke(
        cli,
        ["timeline", "--from", "-1h"],
    )

    assert result.exit_code == 2


def test_timeline_from_after_to(cli_stub_source: Any, cli_runner: Any, monkeypatch: Any) -> None:
    cli_stub_source(monkeypatch, _responder_for({}))

    result = cli_runner.invoke(
        cli,
        ["timeline", "--from", "now", "--to", "-1h"],
    )

    assert result.exit_code != 0


def test_run_timeline_events() -> None:
    labels = {"id": "errors", "group": "g1", "detector": "MAD"}
    series_by_metric = {
        METRIC_OUTSIDE: [_series([0.0, 0.0, 1.0, 1.0, 0.0], labels=labels)],
        METRIC_CHANGE_POINT: [_series([0.0, 0.0, 0.0, 2.0, 2.0], labels=labels)],
        METRIC_SEVERITY: [_series([1.0, 1.5, 3.0, 3.5, 2.0], labels=labels)],
    }

    events = run_timeline(series_by_metric)

    assert len(events) == 2
    anomaly, change_point = events
    assert anomaly["kind"] == "anomaly"
    assert anomaly["time"] == 120.0
    assert anomaly["severity"] == 3.0
    assert change_point["kind"] == "change_point"
    assert change_point["time"] == 180.0
    assert change_point["severity"] == 3.5
    # Sorted ascending by time.
    assert anomaly["time"] < change_point["time"]
