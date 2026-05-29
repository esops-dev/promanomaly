"""Metadata-aware validation lint.

Covers the pure PromQL/metadata analysis, the ``validate --lint-metadata``
CLI surface, and the live surfacing in ``inspect`` / ``top``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from click.testing import CliRunner
from fastapi.testclient import TestClient

from promanomaly import cli as cli_module
from promanomaly.cli import cli
from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.main import Application
from promanomaly.metadata_lint import (
    lint_config_metadata,
    normalise_metadata,
    unrated_counters,
)
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline

COUNTER_META = {"http_requests_total": "counter"}


# ----------------------------------------------------------------------
# Pure analysis
# ----------------------------------------------------------------------
def test_raw_counter_is_flagged() -> None:
    assert unrated_counters("http_requests_total", COUNTER_META) == ["http_requests_total"]


def test_rate_wrapped_counter_is_not_flagged() -> None:
    assert unrated_counters("rate(http_requests_total[5m])", COUNTER_META) == []


def test_nested_rate_is_not_flagged() -> None:
    assert unrated_counters("sum by (job)(rate(http_requests_total[5m]))", COUNTER_META) == []


def test_increase_and_other_counter_funcs_not_flagged() -> None:
    assert unrated_counters("increase(http_requests_total[1h])", COUNTER_META) == []
    assert unrated_counters("irate(http_requests_total[5m])", COUNTER_META) == []


def test_counter_inside_count_is_not_flagged() -> None:
    # `node_load1 / count(node_cpu_seconds_total)` uses the counter only to
    # count cores — the raw value never reaches the detector, so it's safe.
    meta = {"node_cpu_seconds_total": "counter"}
    assert unrated_counters("node_load1 / count(node_cpu_seconds_total)", meta) == []


def test_counter_inside_count_by_is_not_flagged() -> None:
    # The aggregation-modifier form `count by (instance) (metric)` is the
    # one node-exporter's node_load1_per_core uses in the wild.
    meta = {"node_cpu_seconds_total": "counter"}
    promql = 'node_load1 / on (instance) count by (instance) (node_cpu_seconds_total{mode="idle"})'
    assert unrated_counters(promql, meta) == []


def test_gauge_is_never_flagged() -> None:
    assert unrated_counters("node_memory_free_bytes", {"node_memory_free_bytes": "gauge"}) == []


def test_unknown_metric_is_no_opinion() -> None:
    # Absent metadata -> no finding, even for a bare metric.
    assert unrated_counters("some_metric_total", {}) == []


def test_normalise_metadata_accepts_raw_and_reduced() -> None:
    raw = {"m": [{"type": "counter", "help": "x"}]}
    assert normalise_metadata(raw) == {"m": "counter"}
    assert normalise_metadata({"m": "gauge"}) == {"m": "gauge"}


def _config(promql: str) -> Config:
    return Config.model_validate(
        {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://stub"},
            "groups": [
                {
                    "name": "g1",
                    "queries": [{"id": "m1", "promql": promql, "detectors": [{"name": "MAD"}]}],
                }
            ],
        }
    )


def test_lint_config_reports_counter_finding() -> None:
    findings = lint_config_metadata(_config("http_requests_total"), COUNTER_META)
    assert len(findings) == 1
    assert findings[0].metric == "http_requests_total"
    assert findings[0].issue == "counter_not_rated"
    assert findings[0].detectors == ["MAD"]


def test_lint_config_clean_when_rated() -> None:
    findings = lint_config_metadata(_config("rate(http_requests_total[5m])"), COUNTER_META)
    assert findings == []


# ----------------------------------------------------------------------
# validate --lint-metadata CLI
# ----------------------------------------------------------------------
def _write_config(tmp_path: Path, promql: str) -> Path:
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "groups": [
            {
                "name": "g1",
                "queries": [{"id": "m1", "promql": promql, "detectors": [{"name": "MAD"}]}],
            }
        ],
    }
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def _stub_factory(metadata: dict[str, str]) -> Any:
    def factory(*_a: Any, **_k: Any) -> StubSource:
        stub = StubSource()
        stub.metadata = dict(metadata)
        return stub

    return factory


def test_validate_lint_metadata_flags_counter(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(cli_module, "PromQLSource", _stub_factory(COUNTER_META))
    path = _write_config(tmp_path, "http_requests_total")
    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--lint-metadata"])
    assert result.exit_code == 0  # advisory without --strict
    assert "counter_not_rated" in result.output
    assert "http_requests_total" in result.output


def test_validate_lint_metadata_strict_fails(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(cli_module, "PromQLSource", _stub_factory(COUNTER_META))
    path = _write_config(tmp_path, "http_requests_total")
    result = CliRunner().invoke(
        cli, ["validate", "--config", str(path), "--lint-metadata", "--strict"]
    )
    assert result.exit_code == 1


def test_validate_lint_metadata_clean_passes(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(cli_module, "PromQLSource", _stub_factory(COUNTER_META))
    path = _write_config(tmp_path, "rate(http_requests_total[5m])")
    result = CliRunner().invoke(
        cli, ["validate", "--config", str(path), "--lint-metadata", "--strict"]
    )
    assert result.exit_code == 0
    assert '"findings": 0' in result.output


def test_validate_runs_all_checks_even_when_estimate_cost_fails(
    tmp_path: Path, monkeypatch: Any
) -> None:
    # --estimate-cost --strict over budget exits non-zero; the metadata
    # lint must still run and print its report (no short-circuit), and the
    # process must still exit non-zero overall.
    monkeypatch.setattr(cli_module, "PromQLSource", _stub_factory(COUNTER_META))
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "safety": {"max_total_series": 1, "max_series_per_query": 1000},
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {"id": "m1", "promql": "http_requests_total", "detectors": [{"name": "MAD"}]}
                ],
            }
        ],
    }
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(cfg))
    result = CliRunner().invoke(
        cli,
        ["validate", "--config", str(path), "--estimate-cost", "--lint-metadata", "--strict"],
    )
    assert result.exit_code == 1
    # Both reports present: the cardinality overshoot AND the lint finding.
    assert "cardinality_exceeded" in result.output
    assert "counter_not_rated" in result.output


def test_validate_strict_alone_mentions_lint_metadata(tmp_path: Path) -> None:
    path = _write_config(tmp_path, "up")
    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--strict"])
    assert result.exit_code != 0
    assert "--lint-metadata" in result.output


# ----------------------------------------------------------------------
# inspect / top surfacing
# ----------------------------------------------------------------------
def _yaml_config(tmp_path: Path, promql: str) -> Path:
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"listen": ":0"},
        "defaults": {"window": "5m", "step": "15s", "min_points": 5},
        "groups": [
            {
                "name": "g1",
                "queries": [{"id": "m1", "promql": promql, "detectors": [{"name": "MAD"}]}],
            }
        ],
    }
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def test_inspect_endpoint_includes_metadata_lint(tmp_path: Path, monkeypatch: Any) -> None:
    stub = StubSource()
    stub.metadata = COUNTER_META
    stub.respond(lambda _: StubResponse(series=[make_series({"i": "h"}, clean_baseline(n=120))]))
    monkeypatch.setattr("promanomaly.main.PromQLSource", lambda *a, **k: stub)
    path = _yaml_config(tmp_path, "http_requests_total")
    app = Application(Config(**yaml.safe_load(path.read_text())), tmp_path)
    with TestClient(app.build_app()) as client:
        resp = client.get("/debug/inspect", params={"id": "m1"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["metadata_lint"]
    assert body["metadata_lint"][0]["metric"] == "http_requests_total"


def test_top_endpoint_lint_param_returns_hints(tmp_path: Path, monkeypatch: Any) -> None:
    stub = StubSource()
    stub.metadata = COUNTER_META
    stub.respond(lambda _: StubResponse(series=[make_series({"i": "h"}, clean_baseline(n=120))]))
    monkeypatch.setattr("promanomaly.main.PromQLSource", lambda *a, **k: stub)
    path = _yaml_config(tmp_path, "http_requests_total")
    app = Application(Config(**yaml.safe_load(path.read_text())), tmp_path)
    with TestClient(app.build_app()) as client:
        with_lint = client.get("/debug/anomalies", params={"lint": "true"})
        without_lint = client.get("/debug/anomalies", params={"lint": "false"})
    assert with_lint.json()["metadata_lint"][0]["metric"] == "http_requests_total"
    # Default (no extra TSDB query) omits the field entirely.
    assert "metadata_lint" not in without_lint.json()
