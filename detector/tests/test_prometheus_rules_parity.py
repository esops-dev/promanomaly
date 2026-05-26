"""Drift guard between the bundled chart PrometheusRule and the standalone
manifest at ``examples/alerts/promanomaly-rules.yaml``.

Both files are intentionally maintained side by side — one is for Helm
users with kube-prometheus-stack, the other for operators who manage
their alerting tree out of the chart. The test catches the most common
drift (someone adds or renames an alert in one file but not the other)
without trying to assert the two files render byte-identically — they
can't, because the chart parameterises thresholds via values.

The test shells out to ``helm template`` because that's the closest
test to what users actually deploy. If ``helm`` is not on PATH the test
is skipped — the CI ``chart-lint`` job runs the same render and would
fail loudly there anyway.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
STANDALONE = REPO_ROOT / "examples" / "alerts" / "promanomaly-rules.yaml"
CHART_DIR = REPO_ROOT / "charts" / "promanomaly"


def _alert_names_from_rule_doc(doc: dict) -> set[str]:
    names: set[str] = set()
    for group in doc.get("spec", {}).get("groups", []):
        for rule in group.get("rules", []):
            if "alert" in rule:
                names.add(rule["alert"])
    return names


def _load_standalone() -> set[str]:
    with STANDALONE.open() as f:
        return _alert_names_from_rule_doc(yaml.safe_load(f))


def _render_chart() -> set[str]:
    helm = shutil.which("helm")
    if helm is None:
        pytest.skip("helm not on PATH")
    proc = subprocess.run(
        [helm, "template", str(CHART_DIR), "--set", "prometheusRule.enabled=true"],
        check=True,
        capture_output=True,
        text=True,
    )
    names: set[str] = set()
    for doc in yaml.safe_load_all(proc.stdout):
        if not doc:
            continue
        if doc.get("kind") == "PrometheusRule":
            names |= _alert_names_from_rule_doc(doc)
    return names


def test_chart_and_standalone_have_matching_alert_set() -> None:
    standalone = _load_standalone()
    chart = _render_chart()

    missing_from_chart = standalone - chart
    missing_from_standalone = chart - standalone

    assert not missing_from_chart, (
        "alerts present in examples/alerts/promanomaly-rules.yaml but not "
        f"rendered by the Helm chart: {sorted(missing_from_chart)}"
    )
    assert not missing_from_standalone, (
        "alerts rendered by the Helm chart but not in "
        f"examples/alerts/promanomaly-rules.yaml: {sorted(missing_from_standalone)}"
    )


def test_standalone_manifest_is_well_formed() -> None:
    with STANDALONE.open() as f:
        doc = yaml.safe_load(f)
    assert doc["kind"] == "PrometheusRule"
    assert doc["apiVersion"].startswith("monitoring.coreos.com/")
    assert doc["spec"]["groups"], "PrometheusRule must declare at least one group"
    # Every rule has an alert name and an expr — minimum viable structure.
    for group in doc["spec"]["groups"]:
        for rule in group["rules"]:
            assert "alert" in rule
            assert "expr" in rule
            assert rule["expr"].strip(), f"empty expr on alert {rule['alert']!r}"
