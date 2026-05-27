"""Tests for the v0.5 stratified x v0.4 discovery interactions.

Covers:

- The stratified baseline cache ages out per-rendered-id entries when
  a ``discover:``-templated query's rendered id stops appearing,
  bounded by ``safety.discovery.forget_after_runs``. Without this the
  cache would grow unboundedly on workloads with churning identities
  (transient pod names, replica IDs that never recycle).
- ``validate --probe`` surfaces the stratified cost projection for
  discover-templated queries: once at the template level and again per
  rendered variant, so PR-time review catches a runaway
  ``baseline_refresh_interval: 1m`` on the most expensive combination
  (stratified x discovery fan-out) too.
- Boot-time / reload-time warnings fire for misconfigured
  ``baseline_refresh_interval`` values.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml
from click.testing import CliRunner

from promanomaly import cli as cli_module
from promanomaly.cli import cli
from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.source import QuerySeries
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource


def _diurnal_history(*, days: int = 30, step_seconds: float = 300.0) -> pd.DataFrame:
    n = int(days * 86400 / step_seconds)
    base_ts = 1_700_000_000.0 - (1_700_000_000.0 % 86400.0)
    timestamps = base_ts + np.arange(n) * step_seconds
    values = 5.0 * np.sin(2.0 * np.pi * (timestamps % 86400.0) / 86400.0)
    return pd.DataFrame({"timestamp": timestamps, "y": values})


def _discovery_config(*, forget_after: int = 3) -> Config:
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"refresh_interval": "1m"},
        "safety": {
            "max_series_per_query": 100,
            "max_total_series": 1000,
            "discovery": {"forget_after_runs": forget_after},
        },
        "defaults": {
            "window": "5m",
            "step": "5m",
            "min_points": 5,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g_strat_disc",
                "queries": [
                    {
                        "id": "weekly_{{ pod }}",
                        "promql": 'rate(metric{pod="{{ pod }}"}[5m])',
                        "discover": [
                            {"variable": "pod", "probe": "up", "label": "pod"},
                        ],
                        "detectors": [
                            {
                                "name": "DayOfWeekMAD",
                                "params": {"baseline_refresh_interval": "1h"},
                            }
                        ],
                    }
                ],
            }
        ],
    }
    return Config.model_validate(raw)


def _make_runner_with_stub_pods(
    stub_source: StubSource,
    pod_history: dict[str, Iterable[str]],
) -> tuple[Runner, list[str]]:
    """Build a runner whose probe returns a different pod set per run.

    Pop the next set off ``pod_sequence`` per probe call; the templated
    rate(metric) query gets a single-series response for every variant.
    """
    pod_sequence = list(pod_history["sequence"])
    history_calls = {"probe": 0}

    def responder(promql: str) -> StubResponse:
        if promql == "up":
            idx = min(history_calls["probe"], len(pod_sequence) - 1)
            history_calls["probe"] += 1
            pods = pod_sequence[idx]
            return StubResponse(
                series=[
                    QuerySeries(
                        labels={"pod": p},
                        samples=_diurnal_history(days=1),
                    )
                    for p in pods.split(",")
                    if p
                ],
            )
        # Range query for a rendered variant — return a single diurnal
        # series; the labels don't matter for stitching logic, only the
        # data does.
        return StubResponse(
            series=[QuerySeries(labels={"pod": "x"}, samples=_diurnal_history(days=30))]
        )

    stub_source.respond(responder)
    cfg = _discovery_config(forget_after=3)
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)
    return runner, pod_sequence


@pytest.mark.asyncio
async def test_stratified_cache_evicts_disappeared_discovery_ids(
    stub_source: StubSource,
) -> None:
    # Two pods present for run 1; only one for runs 2-5. After
    # forget_after_runs=3 absent runs, the cache entry for the missing
    # pod must be evicted so the cache stays bounded under churn.
    pod_history = {
        "sequence": [
            "pod_a,pod_b",  # run 1 — both pods discovered
            "pod_a",  # run 2 — pod_b absent (miss=1)
            "pod_a",  # run 3 — pod_b absent (miss=2)
            "pod_a",  # run 4 — pod_b absent (miss=3, still in cache)
            "pod_a",  # run 5 — pod_b absent (miss=4 > 3, evict)
        ],
    }
    runner, _ = _make_runner_with_stub_pods(stub_source, pod_history)

    await runner.run_group("g_strat_disc")
    # Both pods registered after run 1.
    misses = runner.discovery_stratified_misses_for_testing()
    assert misses[("g_strat_disc", "weekly_{{ pod }}")] == {"weekly_pod_a": 0, "weekly_pod_b": 0}
    assert ("g_strat_disc", "weekly_pod_b") in runner.stratified_cache_keys_for_testing()

    # Run 2-4: pod_b is absent but still tracked.
    for _ in range(3):
        await runner.run_group("g_strat_disc")
    misses = runner.discovery_stratified_misses_for_testing()
    assert ("g_strat_disc", "weekly_pod_b") in runner.stratified_cache_keys_for_testing()
    assert misses[("g_strat_disc", "weekly_{{ pod }}")]["weekly_pod_b"] == 3

    # Run 5: miss count crosses forget_after_runs=3 — eviction.
    await runner.run_group("g_strat_disc")
    misses = runner.discovery_stratified_misses_for_testing()
    assert ("g_strat_disc", "weekly_pod_b") not in runner.stratified_cache_keys_for_testing()
    assert "weekly_pod_b" not in misses[("g_strat_disc", "weekly_{{ pod }}")]


@pytest.mark.asyncio
async def test_stratified_cache_reactivates_returning_discovery_id(
    stub_source: StubSource,
) -> None:
    # A pod that disappears for a few runs but returns BEFORE
    # forget_after_runs should reset its miss counter rather than get
    # evicted — operator-friendly semantics for transient pod restarts.
    pod_history = {
        "sequence": [
            "pod_a,pod_b",  # run 1
            "pod_a",  # run 2 — pod_b absent (miss=1)
            "pod_a",  # run 3 — pod_b absent (miss=2)
            "pod_a,pod_b",  # run 4 — pod_b back, miss must reset to 0
        ],
    }
    runner, _ = _make_runner_with_stub_pods(stub_source, pod_history)

    for _ in range(4):
        await runner.run_group("g_strat_disc")

    misses = runner.discovery_stratified_misses_for_testing()
    assert ("g_strat_disc", "weekly_pod_b") in runner.stratified_cache_keys_for_testing()
    assert misses[("g_strat_disc", "weekly_{{ pod }}")]["weekly_pod_b"] == 0


@pytest.mark.asyncio
async def test_stratified_cache_unaffected_for_non_discovery_query(
    stub_source: StubSource,
) -> None:
    # Sanity: a non-discover query with a stratified detector must NOT
    # populate the discovery-miss tracker — that surface is reserved
    # for templated queries.
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {"window": "5m", "step": "5m", "min_points": 5},
        "groups": [
            {
                "name": "g_plain",
                "queries": [
                    {
                        "id": "weekly",
                        "promql": "rate(metric[5m])",
                        "detectors": [
                            {
                                "name": "DayOfWeekMAD",
                                "params": {"baseline_refresh_interval": "1h"},
                            }
                        ],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    stub_source.respond(
        lambda _: StubResponse(
            series=[QuerySeries(labels={"a": "b"}, samples=_diurnal_history(days=30))]
        )
    )
    runner = Runner(cfg, stub_source, SnapshotStore(), OperationalMetrics())
    await runner.run_group("g_plain")

    assert runner.discovery_stratified_misses_for_testing() == {}


# ---------------------------------------------------------------------------
# validate --probe stratified summary for discover queries
# ---------------------------------------------------------------------------


def _stub_source_with(monkeypatch: pytest.MonkeyPatch, responder: Any) -> StubSource:
    stub = StubSource()
    stub.respond(responder)
    monkeypatch.setattr(cli_module, "PromQLSource", lambda *_a, **_kw: stub)
    return stub


def test_validate_probe_emits_stratified_summary_for_discover_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # validate --probe must surface the stratified cost projection both
    # at the template level (so operators see the aggregated cost
    # before seeing each expansion) AND on each rendered variant (so
    # the cost is associated with the actual id that would be queried
    # in production).
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"refresh_interval": "1m"},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "weekly_{{ pod }}",
                        "promql": 'rate(metric{pod="{{ pod }}"}[5m])',
                        "discover": [{"variable": "pod", "probe": "up", "label": "pod"}],
                        "detectors": [
                            {
                                "name": "DayOfWeekMAD",
                                "params": {"baseline_refresh_interval": "1h"},
                            }
                        ],
                    }
                ],
            }
        ],
    }
    path = tmp_path / "strat-discovery.yaml"
    path.write_text(yaml.safe_dump(cfg))

    def responder(promql: str) -> StubResponse:
        if promql == "up":
            return StubResponse(
                series=[
                    QuerySeries(labels={"pod": "pod_a"}, samples=_diurnal_history(days=1)),
                    QuerySeries(labels={"pod": "pod_b"}, samples=_diurnal_history(days=1)),
                ]
            )
        return StubResponse(
            series=[QuerySeries(labels={"pod": "x"}, samples=_diurnal_history(days=1))]
        )

    _stub_source_with(monkeypatch, responder)
    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--probe"])
    assert result.exit_code == 0, result.output

    lines = [json.loads(line) for line in result.output.strip().splitlines() if line.strip()]
    # Template-level stratified line.
    template_line = next(
        (line for line in lines if line.get("status") == "stratified_template"), None
    )
    assert template_line is not None, "expected template-level stratified summary"
    assert template_line["stratified"]["baseline_refresh_seconds"] == pytest.approx(3600.0)
    assert template_line["stratified"]["baseline_queries_per_day"] == pytest.approx(24.0)

    # Each rendered variant carries the stratified projection too.
    variants = [line for line in lines if line.get("status") == "ok"]
    assert len(variants) == 2
    for variant in variants:
        assert "stratified" in variant
        assert variant["stratified"]["baseline_refresh_seconds"] == pytest.approx(3600.0)


# ---------------------------------------------------------------------------
# Boot-time stratified-config warnings (Fix D)
# ---------------------------------------------------------------------------


def test_collect_misconfigurations_flags_non_positive_baseline_refresh() -> None:
    from promanomaly.detectors import get as get_detector
    from promanomaly.stratified import collect_misconfigurations

    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"refresh_interval": "1m"},
        "defaults": {"window": "5m", "step": "5m", "min_points": 5},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "weekly",
                        "promql": "rate(m[5m])",
                        "detectors": [
                            {
                                "name": "DayOfWeekMAD",
                                "params": {"baseline_refresh_interval": "0s"},
                            }
                        ],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    findings = collect_misconfigurations(
        cfg.groups,
        get_detector,
        cfg.server.refresh_interval_seconds,
    )
    assert len(findings) == 1
    assert findings[0]["kind"] == "invalid_value"
    assert findings[0]["detector"] == "DayOfWeekMAD"


def test_collect_misconfigurations_flags_below_refresh_interval() -> None:
    from promanomaly.detectors import get as get_detector
    from promanomaly.stratified import collect_misconfigurations

    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"refresh_interval": "1m"},
        "defaults": {"window": "5m", "step": "5m", "min_points": 5},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "weekly",
                        "promql": "rate(m[5m])",
                        "detectors": [
                            {
                                "name": "DayOfWeekMAD",
                                # 30 seconds < refresh_interval=60s — cache
                                # is always stale, defeats the strategy.
                                "params": {"baseline_refresh_interval": "30s"},
                            }
                        ],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    findings = collect_misconfigurations(
        cfg.groups,
        get_detector,
        cfg.server.refresh_interval_seconds,
    )
    assert len(findings) == 1
    assert findings[0]["kind"] == "below_refresh_interval"
    assert findings[0]["refresh_interval_seconds"] == pytest.approx(60.0)


def test_collect_misconfigurations_silent_on_healthy_stratified_config() -> None:
    from promanomaly.detectors import get as get_detector
    from promanomaly.stratified import collect_misconfigurations

    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"refresh_interval": "1m"},
        "defaults": {"window": "5m", "step": "5m", "min_points": 5},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "weekly",
                        "promql": "rate(m[5m])",
                        "detectors": [
                            {
                                "name": "DayOfWeekMAD",
                                "params": {"baseline_refresh_interval": "1h"},
                            }
                        ],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    findings = collect_misconfigurations(
        cfg.groups,
        get_detector,
        cfg.server.refresh_interval_seconds,
    )
    assert findings == []


def test_runner_logs_warning_at_boot_for_bad_stratified_config(
    stub_source: StubSource, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Capture logger calls directly via a stub — avoids the structlog
    # ↔ caplog ↔ stdlib bridge complexity that varies with test
    # ordering and with whether ``configure_logging`` has been called.
    calls: list[tuple[str, dict[str, Any]]] = []

    class _StubLogger:
        def warning(self, event: str, **kwargs: Any) -> None:
            calls.append((event, kwargs))

        def __getattr__(self, _name: str) -> Any:
            return lambda *_a, **_kw: None

    from promanomaly import runner as runner_mod

    monkeypatch.setattr(runner_mod, "logger", _StubLogger())

    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"refresh_interval": "1m"},
        "defaults": {"window": "5m", "step": "5m", "min_points": 5},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "weekly",
                        "promql": "rate(m[5m])",
                        "detectors": [
                            {
                                "name": "DayOfWeekMAD",
                                "params": {"baseline_refresh_interval": "0s"},
                            }
                        ],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    Runner(cfg, stub_source, SnapshotStore(), OperationalMetrics())
    matches = [event for event, _ in calls if event == "stratified_baseline_refresh_invalid"]
    assert matches, f"expected boot warning, got events: {[e for e, _ in calls]}"
