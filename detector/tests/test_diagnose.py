"""Config health and tuning diagnostics (``promanomaly diagnose``)."""

from __future__ import annotations

import json
from typing import Any

from click.testing import CliRunner

from promanomaly import cli as cli_module
from promanomaly.cli import cli
from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.diagnose import (
    diagnose_from_metrics_text,
    evaluate_empty,
    evaluate_firing_rates,
    evaluate_silent_detectors,
    evaluate_warming,
)


# ----------------------------------------------------------------------
# Pure evaluators
# ----------------------------------------------------------------------
def test_firing_rate_bands() -> None:
    rows = [
        ({"id": "a", "group": "g", "detector": "MAD"}, 0.0),
        ({"id": "b", "group": "g", "detector": "Hampel"}, 0.8),
        ({"id": "c", "group": "g", "detector": "IQR"}, 0.2),
    ]
    findings = evaluate_firing_rates(rows, fire_high=0.5)
    cats = {(f.query, f.category) for f in findings}
    assert ("a", "never_fires") in cats
    assert ("b", "fires_often") in cats
    # 0.2 is in the healthy band — no finding.
    assert not any(f.query == "c" for f in findings)


def test_warming_findings() -> None:
    rows = [({"group": "g", "id": "a"}, 3.0), ({"group": "g", "id": "b"}, 0.0)]
    findings = evaluate_warming(rows)
    assert len(findings) == 1
    assert findings[0].query == "a"
    assert findings[0].value == 3.0


def test_empty_findings() -> None:
    rows = [({"group": "g1"}, 0.0), ({"group": "g2"}, 12.0)]
    findings = evaluate_empty(rows)
    assert [f.group for f in findings] == ["g1"]


def _config_with_cohort() -> Config:
    return Config.model_validate(
        {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://stub"},
            "groups": [
                {
                    "name": "g",
                    "queries": [
                        {
                            "id": "m1",
                            "promql": "up",
                            "detectors": [{"name": "MAD"}, {"name": "Cohort"}],
                        }
                    ],
                }
            ],
        }
    )


def test_silent_detectors_flags_unemitting() -> None:
    cfg = _config_with_cohort()
    # MAD emitted; Cohort did not (e.g. cohorts below min_cohort_size).
    findings = evaluate_silent_detectors(cfg, emitting={("g", "MAD")})
    assert len(findings) == 1
    assert findings[0].detector == "Cohort"
    assert "min_cohort_size" in findings[0].detail


# ----------------------------------------------------------------------
# Snapshot mode (parse /metrics text)
# ----------------------------------------------------------------------
_METRICS = """
# HELP anomaly_series_count x
# TYPE anomaly_series_count gauge
anomaly_series_count{group="g1"} 5
anomaly_series_count{group="g2"} 0
# TYPE anomaly_warming_up gauge
anomaly_warming_up{group="g1",id="m1",detector="MAD"} 1
# TYPE anomaly_outside_threshold gauge
anomaly_outside_threshold{group="g1",id="m1",detector="MAD"} 1
anomaly_outside_threshold{group="g1",id="m1",detector="MAD",instance="b"} 1
"""


def test_diagnose_from_metrics_text() -> None:
    report = diagnose_from_metrics_text(_METRICS, fire_high=0.5)
    cats = {f.category for f in report.findings}
    assert report.mode == "snapshot"
    assert "empty_query" in cats  # g2 has 0 series
    assert "warming" in cats
    assert "fires_often" in cats  # both MAD series firing -> 100%
    # Snapshot mode never reports never_fires (no rate over time).
    assert "never_fires" not in cats


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def test_diagnose_requires_exactly_one_source() -> None:
    result = CliRunner().invoke(cli, ["diagnose"])
    assert result.exit_code != 0
    assert "exactly one" in result.output


def test_diagnose_target_mode(monkeypatch: Any) -> None:
    class _Resp:
        text = _METRICS

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr("httpx.get", lambda *a, **k: _Resp())
    result = CliRunner().invoke(cli, ["diagnose", "--target", "http://x", "--output", "json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["mode"] == "snapshot"
    assert any(f["category"] == "empty_query" for f in payload["findings"])


def test_diagnose_tsdb_mode(monkeypatch: Any) -> None:
    class _StubSource:
        def __init__(self, *_a: Any, **_k: Any) -> None:
            pass

        async def start(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def instant_query(self, promql: str) -> list[tuple[dict[str, str], float]]:
            if "anomaly_outside_threshold" in promql:
                return [({"id": "m1", "group": "g", "detector": "MAD"}, 0.0)]
            if "anomaly_warming_up" in promql:
                return []
            if "anomaly_series_count" in promql:
                return [({"group": "g"}, 5.0)]
            return []

    monkeypatch.setattr(cli_module, "PromQLSource", _StubSource)
    result = CliRunner().invoke(
        cli, ["diagnose", "--datasource-url", "http://tsdb", "--window", "24h", "--output", "json"]
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["mode"] == "tsdb"
    assert payload["window"] == "24h"
    assert any(f["category"] == "never_fires" for f in payload["findings"])


def test_diagnose_rejects_bad_window(monkeypatch: Any) -> None:
    class _StubSource:
        def __init__(self, *_a: Any, **_k: Any) -> None:
            pass

    monkeypatch.setattr(cli_module, "PromQLSource", _StubSource)
    result = CliRunner().invoke(
        cli, ["diagnose", "--datasource-url", "http://tsdb", "--window", "24 hours; drop"]
    )
    assert result.exit_code != 0
    assert "invalid --window" in result.output
