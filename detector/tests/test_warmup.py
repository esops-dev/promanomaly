"""Warm-up status endpoint + CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from click.testing import CliRunner
from fastapi.testclient import TestClient

from promanomaly.cli import cli
from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.main import Application
from promanomaly.warmup import compute_warmup, render_table, report_from_payload
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline


def _config(min_points: int = 30, detectors: list[dict[str, Any]] | None = None) -> Config:
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": min_points,
        },
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "m1",
                        "promql": "up",
                        "detectors": detectors or [{"name": "MAD"}],
                    }
                ],
            }
        ],
    }
    return Config.model_validate(raw)


@pytest.mark.asyncio
async def test_warmup_reports_ready_when_enough_points(stub_source: StubSource) -> None:
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "h"}, df)]))
    report = await compute_warmup(config=_config(min_points=30), source=stub_source, now=1000.0)
    q = report.groups[0].queries[0]
    assert q.status == "ready"
    assert q.blocked_by == "none"
    assert q.points_available >= 30


@pytest.mark.asyncio
async def test_warmup_reports_warming_with_eta(stub_source: StubSource) -> None:
    df = clean_baseline(n=10)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "h"}, df)]))
    report = await compute_warmup(config=_config(min_points=30), source=stub_source, now=1000.0)
    q = report.groups[0].queries[0]
    assert q.status == "warming"
    assert q.blocked_by == "min_points"
    assert q.points_available == 10
    assert q.points_needed == 30
    # 20 missing points x 15s step.
    assert q.eta_seconds == pytest.approx(300.0)


@pytest.mark.asyncio
async def test_warmup_blocked_by_lookback_when_no_data(stub_source: StubSource) -> None:
    stub_source.respond(lambda _: StubResponse(series=[]))
    report = await compute_warmup(config=_config(), source=stub_source, now=1000.0)
    q = report.groups[0].queries[0]
    assert q.status == "warming"
    assert q.blocked_by == "lookback"
    assert q.points_available == 0


@pytest.mark.asyncio
async def test_warmup_probe_error_degrades_single_row(stub_source: StubSource) -> None:
    from promanomaly.source import SourceQueryError

    stub_source.respond(lambda _: StubResponse(error=SourceQueryError("boom", reason="timeout")))
    report = await compute_warmup(config=_config(), source=stub_source, now=1000.0)
    q = report.groups[0].queries[0]
    assert q.status == "error"


@pytest.mark.asyncio
async def test_warmup_discovery_query_reports_discovery_blocker(
    stub_source: StubSource,
) -> None:
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {"window": "15m", "step": "15s", "min_points": 30},
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "m_{{ node }}",
                        "promql": 'rate(x{node="{{ node }}"}[5m])',
                        "detectors": [{"name": "MAD"}],
                        "discover": [
                            {"variable": "node", "probe": "count by (node)(up)", "label": "node"}
                        ],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    report = await compute_warmup(config=cfg, source=stub_source, now=1000.0)
    q = report.groups[0].queries[0]
    # Templated query isn't probed; reported ready-but-blocked-on-discovery.
    assert q.status == "ready"
    assert q.blocked_by == "discovery"


@pytest.mark.asyncio
async def test_warmup_coarsens_probe_step_for_long_stratified_window(
    stub_source: StubSource,
) -> None:
    # A stratified detector (DayOfWeekMAD, multi-week lookback) must not
    # make the warm-up probe pull >150k points at the 15s scoring step —
    # the probe step is coarsened so the response stays bounded.
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"i": "h"}, df)]))
    cfg = _config(detectors=[{"name": "DayOfWeekMAD"}])
    await compute_warmup(config=cfg, source=stub_source, now=1000.0)
    probe = stub_source.queries[-1]
    # The multi-week window at the coarsened step yields <= the cap, not
    # the ~160k points a 15s step over 4 weeks would have produced.
    assert probe["window"] / probe["step"] <= 1500 + 1
    # And the step was genuinely coarsened above the 15s scoring step.
    assert probe["step"] > 15.0


def test_render_table_round_trips_payload() -> None:
    raw = {
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "m1",
                        "status": "warming",
                        "points_available": 5,
                        "points_needed": 30,
                        "blocked_by": "min_points",
                        "eta_seconds": 375.0,
                    }
                ],
            }
        ]
    }
    table = render_table(report_from_payload(raw))
    assert "g1" in table
    assert "m1" in table
    assert "warming" in table
    assert "min_points" in table


# ----------------------------------------------------------------------
# Endpoint wiring
# ----------------------------------------------------------------------
def _yaml_config(tmp_path: Path, expose: bool) -> Path:
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"listen": ":0", "expose_warmup_endpoint": expose},
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


def test_warmup_endpoint_404_when_disabled(tmp_path: Path, monkeypatch: Any) -> None:
    stub = StubSource()
    stub.respond(lambda _: StubResponse(series=[make_series({"i": "h"}, clean_baseline(n=120))]))
    monkeypatch.setattr("promanomaly.main.PromQLSource", lambda *a, **k: stub)
    path = _yaml_config(tmp_path, expose=False)
    app = Application(Config(**yaml.safe_load(path.read_text())), tmp_path)
    with TestClient(app.build_app()) as client:
        resp = client.get("/warmup")
    assert resp.status_code == 404


def test_warmup_endpoint_returns_report_when_enabled(tmp_path: Path, monkeypatch: Any) -> None:
    stub = StubSource()
    stub.respond(lambda _: StubResponse(series=[make_series({"i": "h"}, clean_baseline(n=120))]))
    monkeypatch.setattr("promanomaly.main.PromQLSource", lambda *a, **k: stub)
    path = _yaml_config(tmp_path, expose=True)
    app = Application(Config(**yaml.safe_load(path.read_text())), tmp_path)
    built = app.build_app()
    with TestClient(built) as client:
        resp = client.get("/warmup")
    assert resp.status_code == 200
    body = resp.json()
    assert body["groups"][0]["name"] == "g1"
    assert body["groups"][0]["queries"][0]["status"] == "ready"


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def test_warmup_cli_renders_table(monkeypatch: Any) -> None:
    payload = {
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "m1",
                        "status": "ready",
                        "points_available": 60,
                        "points_needed": 30,
                        "blocked_by": "none",
                        "eta_seconds": 0.0,
                    }
                ],
            }
        ]
    }

    class _Resp:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, Any]:
            return payload

    monkeypatch.setattr("httpx.get", lambda *a, **k: _Resp())
    result = CliRunner().invoke(cli, ["warmup", "--target", "http://x"])
    assert result.exit_code == 0
    assert "m1" in result.output


def test_warmup_cli_exit_1_when_still_warming(monkeypatch: Any) -> None:
    payload = {
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "m1",
                        "status": "warming",
                        "points_available": 5,
                        "points_needed": 30,
                        "blocked_by": "min_points",
                        "eta_seconds": 375.0,
                    }
                ],
            }
        ]
    }

    class _Resp:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, Any]:
            return payload

    monkeypatch.setattr("httpx.get", lambda *a, **k: _Resp())
    result = CliRunner().invoke(cli, ["warmup", "--target", "http://x"])
    assert result.exit_code == 1
