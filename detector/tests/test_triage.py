"""Tests for live triage: collect_firing, /debug/anomalies, and the printer."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from promanomaly.anomalies import collect_firing
from promanomaly.cli._top import _print_top_text
from promanomaly.config import CURRENT_API_VERSION, load_config
from promanomaly.main import Application
from promanomaly.state import GroupSnapshot, Sample, SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import point_spike


def _labels(**kw: str) -> tuple[tuple[str, str], ...]:
    return tuple(sorted(kw.items()))


def _seed_store() -> SnapshotStore:
    store = SnapshotStore()
    # Two firing series (different severities) + one non-firing.
    samples = [
        # high-severity firing
        Sample(
            "anomaly_outside_threshold",
            _labels(id="cpu", group="g", detector="MAD", instance="h1"),
            1.0,
        ),
        Sample("anomaly_score", _labels(id="cpu", group="g", detector="MAD", instance="h1"), 5.0),
        Sample(
            "anomaly_severity", _labels(id="cpu", group="g", detector="MAD", instance="h1"), 0.9
        ),
        Sample(
            "anomaly_duration_seconds",
            _labels(id="cpu", group="g", detector="MAD", instance="h1"),
            120.0,
        ),
        Sample(
            "anomaly_type_score",
            _labels(id="cpu", group="g", detector="MAD", instance="h1", type="step_up"),
            5.0,
        ),
        # low-severity firing
        Sample(
            "anomaly_outside_threshold",
            _labels(id="mem", group="g", detector="MAD", instance="h2"),
            1.0,
        ),
        Sample("anomaly_score", _labels(id="mem", group="g", detector="MAD", instance="h2"), 3.1),
        Sample(
            "anomaly_severity", _labels(id="mem", group="g", detector="MAD", instance="h2"), 0.3
        ),
        # non-firing
        Sample(
            "anomaly_outside_threshold",
            _labels(id="disk", group="g", detector="MAD", instance="h3"),
            0.0,
        ),
        Sample("anomaly_score", _labels(id="disk", group="g", detector="MAD", instance="h3"), 1.0),
    ]
    store.write(GroupSnapshot(group="g", timestamp=1.0, samples=samples))
    return store


def test_collect_firing_ranks_by_severity() -> None:
    firing = collect_firing(_seed_store())
    assert [f.id for f in firing] == ["cpu", "mem"]
    assert firing[0].severity == pytest.approx(0.9)
    assert firing[0].type == "step_up"
    assert firing[0].duration_seconds == pytest.approx(120.0)
    assert firing[0].labels == {"instance": "h1"}


def test_collect_firing_min_severity_filter() -> None:
    firing = collect_firing(_seed_store(), min_severity=0.5)
    assert [f.id for f in firing] == ["cpu"]


def test_collect_firing_limit() -> None:
    firing = collect_firing(_seed_store(), limit=1)
    assert len(firing) == 1
    assert firing[0].id == "cpu"


def test_collect_firing_group_filter() -> None:
    assert collect_firing(_seed_store(), group="nope") == []
    assert len(collect_firing(_seed_store(), group="g")) == 2


def test_print_top_text_smoke(capsys: pytest.CaptureFixture[str]) -> None:
    payload = {
        "anomalies": [
            {
                "id": "cpu",
                "group": "g",
                "detector": "MAD",
                "detector_instance": None,
                "score": 5.0,
                "severity": 0.9,
                "duration_seconds": 720.0,
                "type": "step_up",
                "labels": {"instance": "h1"},
            }
        ]
    }
    _print_top_text(payload)
    out = capsys.readouterr().out
    assert "cpu" in out
    assert "step_up" in out
    assert "12m" in out  # 720s formatted


def test_print_top_text_empty(capsys: pytest.CaptureFixture[str]) -> None:
    _print_top_text({"anomalies": []})
    assert "no firing anomalies" in capsys.readouterr().out


def _minimal_yaml(tmp_path: Path) -> Path:
    cfg: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"listen": ":0", "refresh_interval": "30s", "ready_endpoint": True},
        "defaults": {
            "window": "5m",
            "step": "15s",
            "min_points": 5,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g1",
                "queries": [{"id": "m1", "promql": "up", "detectors": [{"name": "MAD"}]}],
            }
        ],
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def test_debug_anomalies_endpoint(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _minimal_yaml(tmp_path)
    stub = StubSource()
    stub.respond(
        lambda _: StubResponse(series=[make_series({"instance": "h"}, point_spike(n=120))])
    )
    monkeypatch.setattr("promanomaly.main.PromQLSource", lambda **kwargs: stub)
    app = Application(load_config(config_path), config_path)
    import time

    with TestClient(app.build_app()) as client:
        for _ in range(50):
            if client.get("/ready").status_code == 200:
                break
            time.sleep(0.05)
        resp = client.get("/debug/anomalies")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] >= 1
        assert body["anomalies"][0]["id"] == "m1"
        assert body["anomalies"][0]["severity"] > 0.0
        # min_severity filter excludes everything above 1.0.
        assert client.get("/debug/anomalies", params={"min_severity": 1.01}).json()["count"] == 0
