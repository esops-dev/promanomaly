"""Regression tests for the round-3 stratified audit fixes.

Covers:

- Backtest widens its history fetch when a stratified detector is in
  the query (otherwise the bucket baseline collapses on a too-short
  slice and reported precision/recall is meaningless).
- ``replace_config`` evicts cache entries when the stratified detector
  is removed from a kept query, when the template PromQL changes for
  a discover query (where cache keys carry rendered ids), and when
  the template id is unchanged.
- The shared misconfig collector flags ``invalid_lookback`` (the
  symmetric companion to ``invalid_value`` for
  ``baseline_refresh_interval``).
- HA mode suppresses the boot-time warning so followers don't repeat
  the leader's message.
- The shipped ``examples/configs/weekly-baseline.yaml`` validates
  against the schema — caught at PR time rather than first install.
- Multi-instance stratified detectors share one cache entry sized to
  ``max(lookback)`` and refreshed at ``min(baseline_refresh_interval)``
  — the documented behaviour in ``docs/scaling-stratified.md``.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pytest

from promanomaly.backtest import run_backtest
from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.detectors import get as get_detector
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.source import QuerySeries
from promanomaly.state import SnapshotStore
from promanomaly.stratified import collect_misconfigurations
from tests.conftest import StubResponse, StubSource


def _diurnal_history(*, days: int, step_seconds: float = 300.0) -> pd.DataFrame:
    n = int(days * 86400 / step_seconds)
    base_ts = 1_700_000_000.0 - (1_700_000_000.0 % 86400.0)
    timestamps = base_ts + np.arange(n) * step_seconds
    values = 5.0 * np.sin(2.0 * np.pi * (timestamps % 86400.0) / 86400.0)
    return pd.DataFrame({"timestamp": timestamps, "y": values})


# ---------------------------------------------------------------------------
# Fix 1: backtest must widen the history fetch for stratified detectors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backtest_widens_history_fetch_for_stratified_detector(
    stub_source: StubSource,
) -> None:
    captured_windows: list[float] = []

    def responder(_: str) -> StubResponse:
        return StubResponse(
            series=[QuerySeries(labels={"instance": "h"}, samples=_diurnal_history(days=30))]
        )

    stub_source.respond(responder)

    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "1h",
            "step": "5m",
            "min_points": 5,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g_strat",
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

    # Capture the window argument used by backtest's range_query — the
    # stub records every call into ``queries``.
    await run_backtest(config=cfg, source=stub_source, group_name="g_strat", inject=False)
    for call in stub_source.queries:
        captured_windows.append(call["window"])
    # Without the fix, backtest would fetch ``defaults.window`` (3600s).
    # With the fix, it widens to ``DayOfWeekMAD.lookback`` (4w == 2,419,200s).
    assert max(captured_windows) >= 4 * 7 * 86400.0, (
        f"backtest must widen to the detector's lookback; got {captured_windows}"
    )


@pytest.mark.asyncio
async def test_backtest_non_stratified_keeps_short_window(
    stub_source: StubSource,
) -> None:
    """Sanity check: a non-stratified-only config must NOT inflate the
    backtest fetch — we'd be hammering the TSDB for no reason."""
    stub_source.respond(
        lambda _: StubResponse(series=[QuerySeries(labels={}, samples=_diurnal_history(days=2))])
    )
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "1m",
            "min_points": 5,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g_plain",
                "queries": [{"id": "m", "promql": "up", "detectors": [{"name": "MAD"}]}],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    await run_backtest(config=cfg, source=stub_source, group_name="g_plain", inject=False)
    # Plain MAD: backtest fetches ``defaults.window`` (900s). No
    # widening should happen.
    assert max(call["window"] for call in stub_source.queries) == 900.0


# ---------------------------------------------------------------------------
# Fix 2: replace_config evicts when stratified detector is removed; doesn't
# evict when the template promql is unchanged for a discover query.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reload_drops_cache_when_stratified_detector_removed(
    stub_source: StubSource,
) -> None:
    stub_source.respond(
        lambda _: StubResponse(
            series=[QuerySeries(labels={"a": "b"}, samples=_diurnal_history(days=30))]
        )
    )
    raw_v1 = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {"window": "5m", "step": "5m", "min_points": 5},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "q",
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
    runner = Runner(
        Config.model_validate(raw_v1), stub_source, SnapshotStore(), OperationalMetrics()
    )
    await runner.run_group("g")
    assert ("g", "q") in runner.stratified_cache_keys_for_testing()

    # Reload: keep the query but drop the stratified detector.
    raw_v2 = dict(raw_v1)
    raw_v2["groups"] = [
        {
            "name": "g",
            "queries": [{"id": "q", "promql": "rate(m[5m])", "detectors": [{"name": "MAD"}]}],
        }
    ]
    runner.replace_config(Config.model_validate(raw_v2))
    assert ("g", "q") not in runner.stratified_cache_keys_for_testing()


@pytest.mark.asyncio
async def test_reload_preserves_discover_cache_when_template_unchanged(
    stub_source: StubSource,
) -> None:
    """Pre-fix bug: every reload wiped discover cache entries because
    the runner compared the rendered promql against the template
    promql (which never matches). The template fields on the cache
    entry make the identity check correct."""

    def responder(promql: str) -> StubResponse:
        if promql == "up":
            return StubResponse(
                series=[QuerySeries(labels={"pod": "pod_a"}, samples=_diurnal_history(days=1))]
            )
        return StubResponse(
            series=[QuerySeries(labels={"pod": "x"}, samples=_diurnal_history(days=30))]
        )

    stub_source.respond(responder)
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {"window": "5m", "step": "5m", "min_points": 5},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "weekly_{{ pod }}",
                        "promql": 'rate(m{pod="{{ pod }}"}[5m])',
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
    cfg = Config.model_validate(raw)
    runner = Runner(cfg, stub_source, SnapshotStore(), OperationalMetrics())
    await runner.run_group("g")
    rendered_key = ("g", "weekly_pod_a")
    assert rendered_key in runner.stratified_cache_keys_for_testing()

    # Reload with byte-identical config — the cache must survive.
    runner.replace_config(Config.model_validate(raw))
    assert rendered_key in runner.stratified_cache_keys_for_testing(), (
        "discover-rendered cache entry was wrongly evicted by a no-op reload"
    )


@pytest.mark.asyncio
async def test_reload_evicts_discover_cache_when_template_promql_changes(
    stub_source: StubSource,
) -> None:
    def responder(promql: str) -> StubResponse:
        if promql == "up":
            return StubResponse(
                series=[QuerySeries(labels={"pod": "pod_a"}, samples=_diurnal_history(days=1))]
            )
        return StubResponse(
            series=[QuerySeries(labels={"pod": "x"}, samples=_diurnal_history(days=30))]
        )

    stub_source.respond(responder)
    raw_v1 = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {"window": "5m", "step": "5m", "min_points": 5},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "weekly_{{ pod }}",
                        "promql": 'rate(m{pod="{{ pod }}"}[5m])',
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
    runner = Runner(
        Config.model_validate(raw_v1), stub_source, SnapshotStore(), OperationalMetrics()
    )
    await runner.run_group("g")
    rendered_key = ("g", "weekly_pod_a")
    assert rendered_key in runner.stratified_cache_keys_for_testing()

    raw_v2 = dict(raw_v1)
    raw_v2["groups"][0]["queries"][0]["promql"] = 'rate(other{pod="{{ pod }}"}[5m])'
    runner.replace_config(Config.model_validate(raw_v2))
    assert rendered_key not in runner.stratified_cache_keys_for_testing(), (
        "template promql change must invalidate all rendered descendants"
    )


# ---------------------------------------------------------------------------
# Fix 5: invalid_lookback is flagged by collect_misconfigurations
# ---------------------------------------------------------------------------


def test_collect_misconfigurations_flags_invalid_lookback() -> None:
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
                                # lookback=0s → query_window_seconds resolves
                                # to 0; the fetch would be empty and the
                                # bucket baseline would collapse.
                                "params": {"lookback": "0s"},
                            }
                        ],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    findings = collect_misconfigurations(
        cfg.groups, get_detector, cfg.server.refresh_interval_seconds
    )
    kinds = {f["kind"] for f in findings}
    assert "invalid_lookback" in kinds


# ---------------------------------------------------------------------------
# Fix 7: HA followers suppress the boot-time warning; leader resurfaces it
# ---------------------------------------------------------------------------


def _bad_stratified_config() -> dict[str, Any]:
    return {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"refresh_interval": "1m"},
        "safety": {"redis": {"url": "redis://localhost:6379/0"}},
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


def _captured_logger_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[str, dict[str, Any]]]:
    """Capture every ``logger.warning(event, **kwargs)`` call in the runner.

    Monkeypatches the runner module's ``logger`` for the duration of
    the test so we don't depend on structlog's PrintLogger /
    LoggerFactory routing, caplog handler installation, or the
    cache_logger_on_first_use semantics. Cleaner than fighting the
    structlog ↔ stdlib bridge in test fixtures.
    """
    calls: list[tuple[str, dict[str, Any]]] = []

    class _StubLogger:
        def warning(self, event: str, **kwargs: Any) -> None:
            calls.append((event, kwargs))

        # Filling out the rest of the structlog surface as no-ops so a
        # detector that logs at any other level during construction
        # doesn't blow up.
        def __getattr__(self, _name: str) -> Any:
            return lambda *_a, **_kw: None

    from promanomaly import runner as runner_mod

    monkeypatch.setattr(runner_mod, "logger", _StubLogger())
    return calls


def test_runner_suppresses_misconfig_warning_in_ha_mode_at_boot(
    stub_source: StubSource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _captured_logger_calls(monkeypatch)
    raw = _bad_stratified_config()
    raw["highAvailability"] = {"enabled": True}
    Runner(
        Config.model_validate(raw),
        stub_source,
        SnapshotStore(),
        OperationalMetrics(),
    )
    matches = [event for event, _ in calls if event == "stratified_baseline_refresh_invalid"]
    assert not matches, (
        "HA-mode boot must not log the stratified warning; the leader-acquisition "
        f"callback surfaces it instead. Got: {[e for e, _ in calls]}"
    )


# ---------------------------------------------------------------------------
# Round-4 follow-ups: shipped example config validates, multi-instance behavior
# ---------------------------------------------------------------------------


def test_shipped_weekly_baseline_example_validates() -> None:
    """``examples/configs/weekly-baseline.yaml`` must validate against
    the schema — caught at PR time rather than at first install."""
    from pathlib import Path

    from promanomaly.config import load_config

    example = Path(__file__).resolve().parents[2] / "examples" / "configs" / "weekly-baseline.yaml"
    assert example.exists(), f"example config missing at {example}"
    cfg = load_config(example)
    # Sanity: the group includes both stratified detectors and a voting
    # ensemble — the value-add of the example is showing all three
    # composing cleanly.
    weekly_group = next(g for g in cfg.groups if g.name == "weekly_business_traffic")
    detector_names = {e.name for q in weekly_group.queries for e in q.detectors}
    assert {"HourOfDayMAD", "DayOfWeekMAD"}.issubset(detector_names)
    assert weekly_group.ensemble is not None
    assert weekly_group.ensemble.method == "voting"


@pytest.mark.asyncio
async def test_multi_instance_stratified_shares_one_cache_entry(
    stub_source: StubSource,
) -> None:
    """Two instances of one stratified detector under the same query
    share a single cache entry sized to ``max(lookback)`` and refreshed
    at ``min(baseline_refresh_interval)``. Verifies the documented
    behaviour in ``docs/scaling-stratified.md`` so a future refactor
    that accidentally splits the cache per-instance gets caught."""
    history = _diurnal_history(days=60)
    stub_source.respond(
        lambda _: StubResponse(series=[QuerySeries(labels={"a": "b"}, samples=history)])
    )
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
                        "id": "q",
                        "promql": "rate(m[5m])",
                        "detectors": [
                            {
                                "name": "DayOfWeekMAD",
                                "instance": "short",
                                "params": {
                                    "lookback": "2w",
                                    "baseline_refresh_interval": "1h",
                                },
                            },
                            {
                                "name": "DayOfWeekMAD",
                                "instance": "long",
                                "params": {
                                    "lookback": "4w",
                                    "baseline_refresh_interval": "30m",
                                },
                            },
                        ],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    runner = Runner(cfg, stub_source, SnapshotStore(), OperationalMetrics())
    await runner.run_group("g")

    # Single cache entry under (group, query.id).
    cache_keys = runner.stratified_cache_keys_for_testing()
    assert cache_keys == {("g", "q")}, f"expected exactly one shared cache entry; got {cache_keys}"

    # The widest baseline window wins. The stub-source records all
    # range_query calls; the second call (after the short fetch) is
    # the baseline fetch and must be the 4w lookback.
    baseline_calls = [c for c in stub_source.queries if c["window"] >= 7 * 86400.0]
    assert baseline_calls, "expected a baseline fetch covering at least one week"
    assert max(c["window"] for c in baseline_calls) == pytest.approx(4 * 7 * 86400.0)


def test_runner_emits_warning_on_explicit_surface_call(
    stub_source: StubSource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Even in HA mode, the leader's acquisition callback calls
    ``surface_misconfigurations()`` directly and expects the warning."""
    raw = _bad_stratified_config()
    raw["highAvailability"] = {"enabled": True}
    runner = Runner(Config.model_validate(raw), stub_source, SnapshotStore(), OperationalMetrics())
    # Patch AFTER construction so the HA-suppressed boot doesn't show
    # up — we want to assert the explicit surface call alone.
    calls = _captured_logger_calls(monkeypatch)
    runner.surface_misconfigurations()
    matches = [event for event, _ in calls if event == "stratified_baseline_refresh_invalid"]
    assert matches, (
        "explicit surface_misconfigurations() call must log the warning; "
        f"got events: {[e for e, _ in calls]}"
    )
