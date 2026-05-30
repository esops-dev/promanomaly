"""`generate-rules` — PrometheusRule scaffolding from a config."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from click.testing import CliRunner

from promanomaly.cli import cli
from promanomaly.cli._generate_rules import generate_prometheus_rules
from promanomaly.config import CURRENT_API_VERSION, Config


def _config(**extra: Any) -> Config:
    base: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://vm"},
        "groups": [
            {
                "name": "errors",
                "queries": [
                    {"id": "http_5xx", "promql": "rate(http_errors[5m])", "detectors": ["MAD"]},
                ],
            }
        ],
    }
    base.update(extra)
    return Config.model_validate(base)


def _rule_groups(manifest: str) -> dict[str, Any]:
    doc = yaml.safe_load(manifest)
    assert doc["kind"] == "PrometheusRule"
    return {g["name"]: g for g in doc["spec"]["groups"]}


def test_generates_valid_yaml_with_recording_and_alert_groups() -> None:
    groups = _rule_groups(generate_prometheus_rules(_config()))
    assert "promanomaly.recording" in groups
    assert "promanomaly.errors" in groups


def test_outside_threshold_selector_is_prefilled() -> None:
    groups = _rule_groups(generate_prometheus_rules(_config()))
    alert = groups["promanomaly.errors"]["rules"][0]
    # Unique, descriptive alert name per (group, query) so the file is
    # duplicate-rule-lint clean and each alert is individually routable.
    assert alert["alert"] == "AnomalyErrorsHttp5xxOutsideThreshold"
    assert alert["expr"] == 'anomaly_outside_threshold{group="errors", id="http_5xx"} == 1'


def test_alert_names_are_unique_across_queries_and_groups() -> None:
    cfg = _config(
        groups=[
            {
                "name": "errors",
                "queries": [
                    {"id": "http_5xx", "promql": "a", "detectors": ["MAD"]},
                    {"id": "http_4xx", "promql": "b", "detectors": ["MAD"]},
                ],
            },
            {
                "name": "latency",
                "queries": [{"id": "http_5xx", "promql": "c", "detectors": ["MAD"]}],
            },
            {
                # Pathological: two distinct ids that camel-case to the same
                # token ("AB"). The dedup pass must still keep names unique.
                "name": "edge",
                "queries": [
                    {"id": "a_b", "promql": "d", "detectors": ["MAD"]},
                    {"id": "a__b", "promql": "e", "detectors": ["MAD"]},
                ],
            },
        ]
    )
    doc = yaml.safe_load(generate_prometheus_rules(cfg))
    names = [r["alert"] for g in doc["spec"]["groups"] for r in g["rules"] if "alert" in r]
    assert len(names) == len(set(names))  # no duplicates
    # The collision was resolved with a numeric suffix, not dropped.
    assert "AnomalyEdgeABOutsideThreshold" in names
    assert "AnomalyEdgeABOutsideThreshold2" in names


def test_recording_rule_is_the_max_ensemble_fallback() -> None:
    groups = _rule_groups(generate_prometheus_rules(_config()))
    rules = groups["promanomaly.recording"]["rules"]
    # A single global rule covers every group; by (id, group) partitions
    # the output per series, so per-group copies would only trip the
    # duplicate-recording-rule lint.
    assert len(rules) == 1
    assert rules[0]["record"] == "anomaly_outside_threshold_any"
    assert rules[0]["expr"] == "max by (id, group) (anomaly_outside_threshold)"


def test_recording_group_name_does_not_collide_with_a_user_group() -> None:
    # A detector group literally named "recording" would otherwise produce
    # two rule groups both named "promanomaly.recording" — which Prometheus
    # rejects as a repeated group name in the same file.
    cfg = _config(
        groups=[
            {"name": "recording", "queries": [{"id": "q", "promql": "up", "detectors": ["MAD"]}]}
        ]
    )
    doc = yaml.safe_load(generate_prometheus_rules(cfg))
    group_names = [g["name"] for g in doc["spec"]["groups"]]
    assert len(group_names) == len(set(group_names))  # no repeated group names


def test_thresholds_are_marked_todo_not_baked_in() -> None:
    manifest = generate_prometheus_rules(_config())
    # Severity / for: / runbook are placeholders, not opinions.
    assert "# TODO: set severity" in manifest
    assert "# TODO: set the flap-suppression window" in manifest


def test_templated_id_falls_back_to_group_scope() -> None:
    cfg = _config(
        groups=[
            {
                "name": "per_node",
                "queries": [
                    {
                        "id": "cpu_{{ instance }}",
                        "promql": 'rate(cpu{instance="{{ instance }}"}[5m])',
                        "detectors": ["MAD"],
                        "discover": [{"variable": "instance", "probe": "up", "label": "instance"}],
                    }
                ],
            }
        ]
    )
    manifest = generate_prometheus_rules(cfg)
    groups = _rule_groups(manifest)
    alert = groups["promanomaly.per_node"]["rules"][0]
    # No literal id selector — scoped by group only.
    assert alert["expr"] == 'anomaly_outside_threshold{group="per_node"} == 1'
    assert "discover-templated" in manifest


def test_ensemble_group_gets_an_ensemble_alert() -> None:
    cfg = _config(
        groups=[
            {
                "name": "errors",
                "ensemble": {"method": "voting", "min_detectors": 2},
                "queries": [
                    {
                        "id": "http_5xx",
                        "promql": "rate(http_errors[5m])",
                        "detectors": [
                            {"name": "MAD", "instance": "short"},
                            {"name": "Hampel", "instance": "long"},
                        ],
                    },
                ],
            }
        ]
    )
    groups = _rule_groups(generate_prometheus_rules(cfg))
    alert_names = [r.get("alert") for r in groups["promanomaly.errors"]["rules"]]
    assert "AnomalyErrorsEnsembleAgreement" in alert_names


def _write_config(tmp_path: Path) -> Path:
    path = tmp_path / "c.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "apiVersion": CURRENT_API_VERSION,
                "datasource": {"url": "http://vm"},
                "groups": [
                    {
                        "name": "errors",
                        "queries": [{"id": "q", "promql": "up", "detectors": ["MAD"]}],
                    }
                ],
            }
        )
    )
    return path


def test_cli_generate_rules_to_stdout(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, ["generate-rules", "--config", str(_write_config(tmp_path))])
    assert result.exit_code == 0
    assert "kind: PrometheusRule" in result.output


def test_cli_generate_rules_to_file(tmp_path: Path) -> None:
    out = tmp_path / "rules.yaml"
    result = CliRunner().invoke(
        cli,
        ["generate-rules", "--config", str(_write_config(tmp_path)), "--output", str(out)],
    )
    assert result.exit_code == 0
    assert yaml.safe_load(out.read_text())["kind"] == "PrometheusRule"


def test_cli_generate_rules_custom_name(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        ["generate-rules", "--config", str(_write_config(tmp_path)), "--name", "my-rules"],
    )
    assert result.exit_code == 0
    assert "name: my-rules" in result.output
