"""End-to-end tests for the FastAPI application boot path.

Exercises everything that ``serve()`` wires together short of actually
running uvicorn: lifespan startup, scheduler tick, /metrics, /healthz,
/ready, /-/reload (with and without bearer auth), and config hot-swap.
The PromQL client is stubbed so the tests stay deterministic and fast.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from promanomaly.config import CURRENT_API_VERSION
from promanomaly.main import Application
from promanomaly.source import PromQLSource
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline


def _minimal_yaml(tmp_path: Path, **overrides: Any) -> Path:
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {
            "listen": ":0",
            "refresh_interval": "30s",
            "ready_endpoint": True,
        },
        "defaults": {
            "window": "5m",
            "step": "15s",
            "min_points": 5,
            "warmup_policy": "emit_warming_up",
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "m1",
                        "promql": "up",
                        "detectors": [{"name": "MAD"}],
                    }
                ],
            }
        ],
    }
    # Shallow-merge overrides at the top level.
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(cfg.get(key), dict):
            cfg[key] = {**cfg[key], **value}  # type: ignore[dict-item]
        else:
            cfg[key] = value
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


@pytest.fixture
def stub_app(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Application, StubSource]:
    """Build an Application with the PromQL client swapped for a stub."""
    config_path = _minimal_yaml(tmp_path)
    stub = StubSource()
    stub.respond(
        lambda _: StubResponse(series=[make_series({"instance": "h"}, clean_baseline(n=120))])
    )

    # Replace PromQLSource constructor used inside Application.
    monkeypatch.setattr(
        "promanomaly.main.PromQLSource",
        lambda **kwargs: stub,
    )
    from promanomaly.config import load_config

    app = Application(load_config(config_path), config_path)
    return app, stub


def test_metrics_and_healthz_after_first_run(stub_app: tuple[Application, StubSource]) -> None:
    app, _stub = stub_app
    with TestClient(app.build_app()) as client:
        assert client.get("/healthz").text == "ok"
        # The lifespan hook fires an immediate first run; by the time
        # TestClient returns control the snapshot is in place.
        ready = client.get("/ready")
        assert ready.status_code == 200
        body = client.get("/metrics").text
        assert "anomaly_score" in body
        assert 'group="g1"' in body
        assert 'detector="MAD"' in body


def test_reload_endpoint_picks_up_yaml_changes(
    stub_app: tuple[Application, StubSource], tmp_path: Path
) -> None:
    app, _stub = stub_app
    with TestClient(app.build_app()) as client:
        # Mutate the on-disk config: add a second group.
        new = yaml.safe_load(Path(app._config_path).read_text())
        new["groups"].append(
            {
                "name": "g2",
                "queries": [{"id": "m2", "promql": "up", "detectors": [{"name": "MAD"}]}],
            }
        )
        Path(app._config_path).write_text(yaml.safe_dump(new))

        resp = client.post("/-/reload")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        # After reload, /debug/config reflects both groups.
        debug = client.get("/debug/config").json()
        assert [g["name"] for g in debug["groups"]] == ["g1", "g2"]


def test_reload_endpoint_rejects_invalid_config(
    stub_app: tuple[Application, StubSource],
) -> None:
    app, _stub = stub_app
    with TestClient(app.build_app()) as client:
        # Corrupt the config on disk — invalid apiVersion.
        broken = yaml.safe_load(Path(app._config_path).read_text())
        broken["apiVersion"] = "promanomaly.io/v999"
        Path(app._config_path).write_text(yaml.safe_dump(broken))

        resp = client.post("/-/reload")
        assert resp.status_code == 400
        # Running config is unchanged.
        debug = client.get("/debug/config").json()
        assert debug["apiVersion"] == CURRENT_API_VERSION


def test_reload_bearer_auth_required(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _minimal_yaml(
        tmp_path,
        server={
            "listen": ":0",
            "refresh_interval": "30s",
            "ready_endpoint": True,
            "reload": {
                "enabled": True,
                "auth": {"type": "bearer", "token": "s3cret"},
            },
        },
    )
    stub = StubSource()
    stub.respond(lambda _: StubResponse(series=[make_series({}, clean_baseline(n=120))]))
    monkeypatch.setattr("promanomaly.main.PromQLSource", lambda **_: stub)
    from promanomaly.config import load_config

    app = Application(load_config(config_path), config_path)
    with TestClient(app.build_app()) as client:
        # Wrong token / no header → 401
        assert client.post("/-/reload").status_code == 401
        bad = client.post("/-/reload", headers={"Authorization": "Bearer wrong"})
        assert bad.status_code == 401
        # Right token → 200
        good = client.post("/-/reload", headers={"Authorization": "Bearer s3cret"})
        assert good.status_code == 200


def test_debug_config_redacts_secrets(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _minimal_yaml(
        tmp_path,
        datasource={
            "url": "http://stub",
            "auth": {"type": "bearer", "token": "shh"},
        },
    )
    stub = StubSource()
    stub.respond(lambda _: StubResponse(series=[]))
    monkeypatch.setattr("promanomaly.main.PromQLSource", lambda **_: stub)
    from promanomaly.config import load_config

    app = Application(load_config(config_path), config_path)
    with TestClient(app.build_app()) as client:
        view = client.get("/debug/config").json()
        assert view["datasource"]["auth"]["token"] == "<redacted>"


def test_promqlsource_unused_import_kept() -> None:
    # Lightweight sanity check that the real client class is importable
    # even though every test in this file stubs it out.
    assert PromQLSource is not None


def test_debug_detectors_endpoint_lists_built_ins(
    stub_app: tuple[Application, StubSource],
) -> None:
    app, _stub = stub_app
    with TestClient(app.build_app()) as client:
        resp = client.get("/debug/detectors")
        assert resp.status_code == 200
        payload = resp.json()
        names = {entry["name"] for entry in payload}
        # Every built-in detector should be visible from the endpoint —
        # operators rely on this to confirm their custom image actually
        # registered what they expect.
        assert {"MAD", "Hampel", "ZScoreEWMA", "IQR", "HourOfDayMAD"}.issubset(names)
        mad = next(entry for entry in payload if entry["name"] == "MAD")
        # Description, defaults, and the canonical params list all come
        # back. The CLI --target proxies the body verbatim, so this
        # shape IS the public contract.
        assert isinstance(mad["description"], str) and mad["description"]
        assert isinstance(mad["defaults"], dict)
        assert isinstance(mad["params"], list)
        # IQR exposes a single ``k`` tunable with type/doc — pin the
        # detailed shape on one detector that has parameters.
        iqr = next(entry for entry in payload if entry["name"] == "IQR")
        params = {p["name"]: p for p in iqr["params"]}
        assert set(params) == {"k"}
        assert params["k"]["type"] == "float"
        assert params["k"]["default"] == 1.5
        assert params["k"]["doc"]


def test_debug_inspect_returns_per_detector_scores(
    stub_app: tuple[Application, StubSource],
) -> None:
    # Live integration check: hit /debug/inspect through the FastAPI
    # app stack (route → inspect_series → stub source) so the wiring
    # — JSON shape, status code, label-selector parsing — is pinned
    # end-to-end, not just at the function level.
    app, _stub = stub_app
    with TestClient(app.build_app()) as client:
        resp = client.get(
            "/debug/inspect",
            params={"id": "m1", "labels": "instance=h"},
        )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["status"] == "ok"
        assert payload["matched_series"] == 1
        assert payload["series"][0]["labels"] == {"instance": "h"}
        names = {d["name"] for d in payload["series"][0]["detectors"]}
        assert "MAD" in names


def test_debug_inspect_unknown_id_returns_400(
    stub_app: tuple[Application, StubSource],
) -> None:
    app, _stub = stub_app
    with TestClient(app.build_app()) as client:
        resp = client.get("/debug/inspect", params={"id": "does_not_exist"})
        # Unknown-id is an operator error, not a server error — the
        # status-code mapping in main.py must keep this on 4xx so CI
        # gates and dashboards don't trigger 5xx alerts on operator
        # typos.
        assert resp.status_code == 400
        body = resp.json()
        assert body["status"] == "error"
        assert body["reason"] == "unknown_id"


def test_debug_inspect_bad_selector_returns_400(
    stub_app: tuple[Application, StubSource],
) -> None:
    app, _stub = stub_app
    with TestClient(app.build_app()) as client:
        resp = client.get(
            "/debug/inspect",
            params={"id": "m1", "labels": "missing_equals_sign"},
        )
        assert resp.status_code == 400
        assert resp.json()["reason"] == "bad_selector"
