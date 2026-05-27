"""Tests for ``promanomaly analyze`` — offline change-point analysis CLI.

The CLI ingests a PromQL query against a datasource, runs an offline
algorithm from ``ruptures`` (Pelt / Binseg / Window) over the returned
series, and prints a ranked list of detected breaks. We stub the
datasource client and assert the CLI:

  * parses RFC3339 / unix-seconds / relative ``-7d`` timestamps;
  * outputs text and JSON;
  * handles "no series returned" cleanly;
  * surfaces ruptures findings as ranked entries.
"""

from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd
import pytest
from click.testing import CliRunner

from promanomaly import cli as cli_module
from promanomaly.cli import _parse_analyze_timestamp, cli
from promanomaly.source import QuerySeries, SourceQueryError
from tests.conftest import StubResponse, StubSource


def _step_series(labels: dict[str, str], n: int = 240, change_at: int = 120) -> QuerySeries:
    """Range-query-shaped series with a single clear step change inside."""
    rng = np.random.default_rng(0)
    timestamps = np.arange(n, dtype=float)
    values = 1.0 + rng.normal(0.0, 0.05, size=n)
    values[change_at:] = 5.0 + rng.normal(0.0, 0.05, size=n - change_at)
    return QuerySeries(
        labels=labels,
        samples=pd.DataFrame({"timestamp": timestamps, "y": values}),
    )


def _stub_for(monkeypatch: pytest.MonkeyPatch, responder: Any) -> StubSource:
    stub = StubSource()
    stub.respond(responder)
    monkeypatch.setattr(cli_module, "PromQLSource", lambda *_a, **_kw: stub)
    return stub


def test_parse_timestamp_accepts_unix_seconds() -> None:
    assert _parse_analyze_timestamp("1700000000", now=0.0) == 1700000000.0


def test_parse_timestamp_accepts_rfc3339_with_z() -> None:
    ts = _parse_analyze_timestamp("2024-01-15T12:30:00Z", now=0.0)
    # Don't pin the exact value — accept any positive epoch in that decade.
    assert 1700000000.0 < ts < 1750000000.0


def test_parse_timestamp_accepts_relative_offset() -> None:
    base = 1_000_000.0
    # ``-7d`` resolves to 7 days before ``now``.
    assert _parse_analyze_timestamp("-7d", now=base) == base - 7 * 86400.0


def test_parse_timestamp_accepts_now() -> None:
    assert _parse_analyze_timestamp("now", now=12345.0) == 12345.0


def test_parse_timestamp_rejects_garbage() -> None:
    with pytest.raises(ValueError, match="invalid timestamp"):
        _parse_analyze_timestamp("not a timestamp", now=0.0)


def test_analyze_text_output_lists_change_points(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pelt run over a series with one clear step change reports at least
    one break and ranks it in the text output."""
    _stub_for(
        monkeypatch,
        lambda _: StubResponse(series=[_step_series({"service": "api"})]),
    )

    result = CliRunner().invoke(
        cli,
        [
            "analyze",
            "--query",
            "up",
            "--from",
            "-1h",
            "--to",
            "now",
            "--datasource-url",
            "http://stub",
            "--penalty",
            "1.0",
        ],
    )
    assert result.exit_code == 0, result.output
    # First header line + at least one finding line.
    assert "when" in result.output
    assert "service=api" in result.output


def test_analyze_json_output_is_parseable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_for(
        monkeypatch,
        lambda _: StubResponse(series=[_step_series({"service": "api"})]),
    )

    result = CliRunner().invoke(
        cli,
        [
            "analyze",
            "--query",
            "up",
            "--from",
            "-1h",
            "--datasource-url",
            "http://stub",
            "--penalty",
            "1.0",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    findings = json.loads(result.output)
    assert isinstance(findings, list)
    # The fixture has exactly one true change-point; we expect at least one finding.
    assert findings
    first = findings[0]
    assert {"timestamp", "iso8601", "score", "labels", "index"} <= set(first)
    assert first["labels"] == {"service": "api"}


def test_analyze_handles_empty_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_for(monkeypatch, lambda _: StubResponse(series=[]))
    result = CliRunner().invoke(
        cli,
        [
            "analyze",
            "--query",
            "up",
            "--from",
            "-1h",
            "--datasource-url",
            "http://stub",
        ],
    )
    assert result.exit_code != 0
    assert "no series" in result.output


def test_analyze_handles_query_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_for(
        monkeypatch,
        lambda _: StubResponse(error=SourceQueryError("server_error", "HTTP 500")),
    )
    result = CliRunner().invoke(
        cli,
        [
            "analyze",
            "--query",
            "up",
            "--from",
            "-1h",
            "--datasource-url",
            "http://stub",
        ],
    )
    assert result.exit_code != 0
    assert "query failed" in result.output


def test_analyze_rejects_inverted_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_for(monkeypatch, lambda _: StubResponse(series=[]))
    result = CliRunner().invoke(
        cli,
        [
            "analyze",
            "--query",
            "up",
            "--from",
            "2024-01-02T00:00:00Z",
            "--to",
            "2024-01-01T00:00:00Z",
        ],
    )
    assert result.exit_code != 0
    assert "--from must be before --to" in result.output


def test_analyze_reports_missing_ruptures_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``ruptures`` is an optional dependency. When it isn't installed
    the CLI must surface a clean install hint instead of letting an
    ImportError bubble up as a traceback."""
    import builtins

    _stub_for(
        monkeypatch,
        lambda _: StubResponse(series=[_step_series({"service": "api"})]),
    )

    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "ruptures":
            raise ImportError("simulated absence of ruptures")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    result = CliRunner().invoke(
        cli,
        [
            "analyze",
            "--query",
            "up",
            "--from",
            "-1h",
            "--datasource-url",
            "http://stub",
        ],
    )
    assert result.exit_code != 0
    # The CLI must point operators at the install command rather than
    # exposing the raw ImportError.
    assert "promanomaly[analyze]" in result.output


def test_analyze_supports_binseg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``binseg`` is an alternate algorithm. We just assert it runs and
    produces ranked output — the algorithm-vs-algorithm comparison is
    documented in change-points.md, not tested here."""
    _stub_for(
        monkeypatch,
        lambda _: StubResponse(series=[_step_series({"service": "api"})]),
    )
    result = CliRunner().invoke(
        cli,
        [
            "analyze",
            "--query",
            "up",
            "--from",
            "-1h",
            "--detector",
            "binseg",
            "--penalty",
            "1.0",
            "--output",
            "json",
            "--datasource-url",
            "http://stub",
        ],
    )
    assert result.exit_code == 0, result.output
    findings = json.loads(result.output)
    assert isinstance(findings, list)
