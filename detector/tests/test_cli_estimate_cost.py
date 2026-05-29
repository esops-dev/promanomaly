"""``validate --estimate-cost`` — static cardinality / cost projection."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from click.testing import CliRunner

from promanomaly.cli import cli
from promanomaly.cli._cost import estimate_cost
from promanomaly.config import CURRENT_API_VERSION, Config


def _write_config(tmp_path: Path, **extras: Any) -> Path:
    cfg: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "groups": [
            {
                "name": "g",
                "queries": [{"id": "m", "promql": "up", "detectors": [{"name": "MAD"}]}],
            }
        ],
    }
    cfg.update(extras)
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def _summary_line(output: str) -> dict[str, Any]:
    for line in output.splitlines():
        obj = json.loads(line)
        if "summary" in obj:
            return obj["summary"]
    raise AssertionError("no summary line in output")


def test_estimate_cost_emits_per_group_and_summary(tmp_path: Path) -> None:
    path = _write_config(tmp_path)
    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--estimate-cost"])
    assert result.exit_code == 0
    summary = _summary_line(result.output)
    # One group, one query, projected at the default per-query cap (1000).
    assert summary["projected_total_series"] == 1000
    assert summary["max_total_series"] == 20000
    assert summary["over_budget"] is False
    assert summary["short_queries_per_refresh"] == 1
    assert summary["estimated_cpu_cores"] >= 0.0
    assert summary["estimated_memory_bytes"] > 0


def test_estimate_cost_does_not_touch_datasource(tmp_path: Path) -> None:
    # The datasource URL is unreachable; --estimate-cost must still
    # succeed because it is a purely static projection.
    path = _write_config(tmp_path, datasource={"url": "http://127.0.0.1:1/unreachable"})
    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--estimate-cost"])
    assert result.exit_code == 0


def test_estimate_cost_strict_exits_nonzero_over_budget(tmp_path: Path) -> None:
    # Tighten the global cap below the per-query worst case so the
    # projection overshoots.
    path = _write_config(
        tmp_path,
        safety={"max_series_per_query": 1000, "max_total_series": 500},
    )
    result = CliRunner().invoke(
        cli, ["validate", "--config", str(path), "--estimate-cost", "--strict"]
    )
    assert result.exit_code == 1
    summary = _summary_line(result.output)
    assert summary["over_budget"] is True


def test_estimate_cost_over_budget_without_strict_is_advisory(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path,
        safety={"max_series_per_query": 1000, "max_total_series": 500},
    )
    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--estimate-cost"])
    # Advisory only: over-budget is reported but the process exits 0.
    assert result.exit_code == 0
    assert _summary_line(result.output)["over_budget"] is True


def test_strict_alone_still_requires_probe_or_estimate(tmp_path: Path) -> None:
    path = _write_config(tmp_path)
    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--strict"])
    assert result.exit_code != 0
    assert "--strict requires" in result.output


def test_estimate_cost_counts_discovery_probe_queries() -> None:
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "m_{{ node }}",
                        "promql": 'rate(http_requests_total{node="{{ node }}"}[5m])',
                        "detectors": [{"name": "MAD"}],
                        "discover": [
                            {
                                "variable": "node",
                                "probe": "count by (node) (up)",
                                "label": "node",
                            }
                        ],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    groups = estimate_cost(cfg)
    assert groups[0].discovery_probe_queries_per_refresh == 1
    assert groups[0].queries[0].is_discovery is True


def test_estimate_cost_flags_discovery_fanout_as_lower_bound(tmp_path: Path) -> None:
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "m_{{ node }}",
                        "promql": 'rate(http_requests_total{node="{{ node }}"}[5m])',
                        "detectors": [{"name": "MAD"}],
                        "discover": [
                            {
                                "variable": "node",
                                "probe": "count by (node) (up)",
                                "label": "node",
                            }
                        ],
                    }
                ],
            }
        ],
    }
    path = tmp_path / "disc.yaml"
    path.write_text(yaml.safe_dump(raw))
    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--estimate-cost"])
    assert result.exit_code == 0
    summary = _summary_line(result.output)
    # Discovery fan-out is unbounded statically, so the total is a lower
    # bound — the operator must not read "under budget" as a guarantee.
    assert summary["projected_series_is_lower_bound"] is True
    assert "discovery_note" in summary
