"""End-to-end detection self-test / dead-man's-switch."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.main import Application
from promanomaly.selftest import run_selftest
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline


def test_selftest_passes_with_default_detector() -> None:
    result = run_selftest(detector_name="MAD", threshold=3.0, min_points=60)
    assert result.ok is True
    assert result.outside is True
    assert result.score >= 3.0
    assert result.detail == "ok"


def test_selftest_fails_with_absurd_threshold() -> None:
    # A threshold no synthetic spike can cross simulates a global
    # mis-tuning — the dead-man's-switch must report it, not pass.
    result = run_selftest(detector_name="MAD", threshold=1e9, min_points=60)
    assert result.ok is False
    assert result.outside is False
    assert "not caught" in result.detail


def test_selftest_unknown_detector_is_failure() -> None:
    result = run_selftest(detector_name="DoesNotExist", threshold=3.0, min_points=60)
    assert result.ok is False
    assert "unknown detector" in result.detail


def _selftest_config(detector: str) -> dict[str, Any]:
    return {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"selftest": {"enabled": True, "detector": detector}},
        "groups": [
            {
                "name": "g1",
                "queries": [{"id": "m1", "promql": "up", "detectors": [{"name": "MAD"}]}],
            }
        ],
    }


def test_config_rejects_stratified_selftest_detector(tmp_path: Path) -> None:
    from promanomaly.config import load_config

    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(_selftest_config("DayOfWeekMAD")))
    with pytest.raises(ValueError, match="stratified"):
        load_config(path)


def test_config_rejects_cohort_selftest_detector(tmp_path: Path) -> None:
    from promanomaly.config import load_config

    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(_selftest_config("Cohort")))
    with pytest.raises(ValueError, match="cohort"):
        load_config(path)


def test_config_allows_rolling_window_selftest_detector(tmp_path: Path) -> None:
    from promanomaly.config import load_config

    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(_selftest_config("Hampel")))
    cfg = load_config(path)
    assert cfg.server.selftest.detector == "Hampel"


def _yaml_config(tmp_path: Path, *, enabled: bool, threshold: float = 3.0) -> Path:
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {
            "listen": ":0",
            "selftest": {"enabled": enabled, "detector": "MAD", "threshold": threshold},
        },
        "defaults": {"window": "5m", "step": "15s", "min_points": 5},
        "groups": [
            {
                "name": "g1",
                "queries": [{"id": "m1", "promql": "up", "detectors": [{"name": "MAD"}]}],
            }
        ],
    }
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def _app(tmp_path: Path, monkeypatch: Any, *, enabled: bool, threshold: float = 3.0) -> Application:
    stub = StubSource()
    stub.respond(lambda _: StubResponse(series=[make_series({"i": "h"}, clean_baseline(n=120))]))
    monkeypatch.setattr("promanomaly.main.PromQLSource", lambda *a, **k: stub)
    path = _yaml_config(tmp_path, enabled=enabled, threshold=threshold)
    return Application(Config(**yaml.safe_load(path.read_text())), tmp_path)


def test_selftest_metric_absent_when_disabled(tmp_path: Path, monkeypatch: Any) -> None:
    app = _app(tmp_path, monkeypatch, enabled=False)
    with TestClient(app.build_app()) as client:
        body = client.get("/metrics").text
    # Disabled: the labelled series never appears, so AnomalyPipelineDead
    # ( anomaly_selftest_ok == 0 ) can't fire on an opted-out deployment.
    assert "anomaly_selftest_ok{" not in body


def test_selftest_emits_ok_when_enabled_and_passing(tmp_path: Path, monkeypatch: Any) -> None:
    app = _app(tmp_path, monkeypatch, enabled=True)
    app._run_selftest()
    body = app._exporter.render()[0].decode("utf-8")
    assert 'anomaly_selftest_ok{detector="MAD"} 1.0' in body


def test_selftest_emits_zero_and_bumps_counter_on_failure(tmp_path: Path, monkeypatch: Any) -> None:
    app = _app(tmp_path, monkeypatch, enabled=True, threshold=1e9)
    app._run_selftest()
    body = app._exporter.render()[0].decode("utf-8")
    assert 'anomaly_selftest_ok{detector="MAD"} 0.0' in body
    assert 'anomaly_selftest_failures_total{detector="MAD"} 1.0' in body
