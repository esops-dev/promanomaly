"""Tests for the live introspection endpoint + ``promanomaly inspect`` CLI."""

from __future__ import annotations

from typing import Any

import pytest
from click.testing import CliRunner

from promanomaly.cli import cli
from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.inspect import InspectError, inspect_series, parse_label_selector
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline, point_spike


def _config(detectors: list[dict[str, Any]] | None = None) -> Config:
    detectors = detectors or [{"name": "MAD"}]
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
        },
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {"id": "metric_a", "promql": "up{job='x'}", "detectors": detectors},
                ],
            }
        ],
    }
    return Config.model_validate(raw)


def test_parse_label_selector_handles_empty_and_keys() -> None:
    assert parse_label_selector(None) == {}
    assert parse_label_selector("") == {}
    assert parse_label_selector("instance=host-a") == {"instance": "host-a"}
    assert parse_label_selector("a=1, b=2") == {"a": "1", "b": "2"}


def test_parse_label_selector_strips_matching_quotes() -> None:
    # /metrics renders label values with quotes and operators think in
    # PromQL, so a copy-pasted quoted selector must match the same series
    # as the bare form rather than silently matching nothing.
    assert parse_label_selector('endpoint="/api/users"') == {"endpoint": "/api/users"}
    assert parse_label_selector("endpoint='/api/users'") == {"endpoint": "/api/users"}
    assert parse_label_selector('a="1", b=2') == {"a": "1", "b": "2"}
    # Mismatched / non-enclosing quotes are left untouched.
    assert parse_label_selector('a="1') == {"a": '"1'}
    assert parse_label_selector('a=1"') == {"a": '1"'}


def test_parse_label_selector_rejects_bare_key() -> None:
    with pytest.raises(InspectError) as exc:
        parse_label_selector("nope")
    assert exc.value.reason == "bad_selector"


@pytest.mark.asyncio
async def test_inspect_unknown_id_raises(stub_source: StubSource) -> None:
    cfg = _config()
    store = SnapshotStore()
    with pytest.raises(InspectError) as exc:
        await inspect_series(
            config=cfg,
            source=stub_source,
            store=store,
            metric_id="not_a_metric",
        )
    assert exc.value.reason == "unknown_id"


@pytest.mark.asyncio
async def test_inspect_discovery_templated_id_returns_actionable_error(
    stub_source: StubSource,
) -> None:
    # Inspect can't yet resolve rendered ids back to discovery-templated
    # queries — operators inspect ``cpu_busy_node_17`` but the config
    # carries ``cpu_busy_{{ instance }}``. Surface a discovery-specific
    # error rather than masquerading as "unknown_id" so the CLI can
    # point users at ``detect-once`` instead.
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {"window": "15m", "step": "15s", "min_points": 30},
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "cpu_busy_{{ instance }}",
                        "promql": 'rate(cpu{instance="{{ instance }}"}[5m])',
                        "discover": [
                            {
                                "variable": "instance",
                                "probe": "count by (instance) (up)",
                                "label": "instance",
                            }
                        ],
                        "detectors": [{"name": "MAD"}],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    with pytest.raises(InspectError) as exc:
        await inspect_series(
            config=cfg,
            source=stub_source,
            store=SnapshotStore(),
            metric_id="cpu_busy_{{ instance }}",
        )
    assert exc.value.reason == "discovery_templated"
    assert "detect-once" in exc.value.message


@pytest.mark.asyncio
async def test_inspect_returns_per_detector_scores(stub_source: StubSource) -> None:
    df = point_spike(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "host-a"}, df)]))
    cfg = _config(detectors=[{"name": "MAD"}, {"name": "Hampel"}])
    store = SnapshotStore()

    payload = await inspect_series(
        config=cfg,
        source=stub_source,
        store=store,
        metric_id="metric_a",
    )
    assert payload["status"] == "ok"
    assert payload["matched_series"] == 1
    assert len(payload["series"]) == 1
    series = payload["series"][0]
    assert series["labels"] == {"instance": "host-a"}
    assert series["sample_count"] == 120
    names = {d["name"] for d in series["detectors"]}
    assert names == {"MAD", "Hampel"}
    mad = next(d for d in series["detectors"] if d["name"] == "MAD")
    assert mad["status"] == "ok"
    # Point-spike fixture is well above 3 MADs from the baseline.
    assert mad["score"] > 3.0
    assert mad["statistical_outside"] is True


@pytest.mark.asyncio
async def test_inspect_filters_by_label_selector(stub_source: StubSource) -> None:
    df = clean_baseline()
    stub_source.respond(
        lambda _: StubResponse(
            series=[
                make_series({"instance": "host-a"}, df),
                make_series({"instance": "host-b"}, df),
            ]
        )
    )
    cfg = _config()
    store = SnapshotStore()
    payload = await inspect_series(
        config=cfg,
        source=stub_source,
        store=store,
        metric_id="metric_a",
        label_selector="instance=host-b",
    )
    assert payload["matched_series"] == 1
    assert payload["total_series"] == 2
    assert payload["series"][0]["labels"]["instance"] == "host-b"


@pytest.mark.asyncio
async def test_inspect_reports_warming_up_state(stub_source: StubSource) -> None:
    # Series with fewer than min_points samples -> detectors report
    # ``warming_up`` rather than running.
    short = clean_baseline(n=5)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "host-a"}, short)]))
    cfg = _config()
    store = SnapshotStore()
    payload = await inspect_series(
        config=cfg,
        source=stub_source,
        store=store,
        metric_id="metric_a",
    )
    series = payload["series"][0]
    assert series["warming_up"] is True
    assert series["sample_count"] == 5
    for det in series["detectors"]:
        assert det["status"] == "warming_up"
        assert "score" not in det


@pytest.mark.asyncio
async def test_inspect_caps_returned_window(stub_source: StubSource) -> None:
    # Stratified-detector style: a fat 1000-sample series should be
    # trimmed in the response so /debug/inspect stays cheap.
    big = clean_baseline(n=1000)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "host-a"}, big)]))
    cfg = _config()
    store = SnapshotStore()
    payload = await inspect_series(
        config=cfg,
        source=stub_source,
        store=store,
        metric_id="metric_a",
    )
    series = payload["series"][0]
    assert len(series["window"]) <= 500
    # Sample count reflects the full series; the trim is just for the
    # HTTP response payload.
    assert series["sample_count"] == 1000


def test_inspect_cli_missing_target_fails() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["inspect", "--id", "metric_a"])
    # --target is required; click should exit non-zero.
    assert result.exit_code != 0


@pytest.mark.asyncio
async def test_inspect_builds_cohort_context_for_cohort_aware_detector(
    stub_source: StubSource,
) -> None:
    # Cohort detectors need cross-member visibility; without the inspect
    # cohort-context pre-pass, inspecting one member would always score 0
    # because the detector wouldn't have its baseline injected. Pin that
    # the inspect path mirrors the runner's per-cohort baseline build.
    import numpy as np
    import pandas as pd

    n = 40
    members = [
        ("node-a", 10.0),
        ("node-b", 10.1),
        ("node-c", 9.9),
        ("node-d", 10.05),
        ("node-e", 9.95),
        ("node-bad", 30.0),
    ]
    series = []
    for name, mean in members:
        rng = np.random.default_rng(hash(name) & 0xFFFFFFFF)
        values = mean + rng.normal(0.0, 0.05, size=n)
        df = pd.DataFrame({"timestamp": np.arange(n, dtype=float) * 15.0, "y": values})
        series.append(make_series({"job": "node", "instance": name}, df))
    stub_source.respond(lambda _: StubResponse(series=series))
    cfg = _config(detectors=[{"name": "Cohort"}])
    store = SnapshotStore()

    payload = await inspect_series(
        config=cfg,
        source=stub_source,
        store=store,
        metric_id="metric_a",
        label_selector="instance=node-bad",
    )
    assert payload["status"] == "ok"
    assert payload["matched_series"] == 1
    detector = payload["series"][0]["detectors"][0]
    assert detector["name"] == "Cohort"
    assert detector["status"] == "ok"
    # cohort_label opt-in label appears on the inspect view too.
    assert detector["cohort_label"] == "instance"
    # The pre-pass found a 6-member cohort, well above the default
    # min_cohort_size of 5.
    assert detector["cohort_size"] == 6
    # The divergent member should score above the default threshold of 3.
    assert detector["score"] > 3.0
