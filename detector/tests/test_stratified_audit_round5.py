"""Regression tests for the round-5 stratified audit fixes.

Covers:

- ``detect-once --detector DayOfWeekMAD`` auto-widens the fetch to
  the detector's preferred lookback when ``--window auto`` (the new
  default). Pre-fix the CLI used a fixed ``1h`` and the bucket
  baseline collapsed.
- ``inspect.py`` uses the shared ``query_window_seconds_for`` helper
  so a detector that raises from ``query_window_seconds`` falls
  back to the group default instead of crashing the
  ``/debug/inspect`` endpoint.
- ``replace_config`` keeps suppressing the stratified misconfig
  warning when the new config still has HA enabled — the boot-time
  test alone would miss a refactor that broke reload-time
  suppression.
- The backtest fetch multiplier is capped at 2x for stratified
  detectors so ``make backtest-inject GROUP=...`` (default ``--from``)
  doesn't ask the TSDB for 16 weeks of data on a 4w lookback.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pytest
from click.testing import CliRunner

from promanomaly import cli as cli_module
from promanomaly.backtest import run_backtest
from promanomaly.cli import cli
from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.source import QuerySeries
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource


def _diurnal_history(*, days: int, step_seconds: float = 300.0) -> pd.DataFrame:
    n = int(days * 86400 / step_seconds)
    base_ts = 1_700_000_000.0 - (1_700_000_000.0 % 86400.0)
    timestamps = base_ts + np.arange(n) * step_seconds
    values = 5.0 * np.sin(2.0 * np.pi * (timestamps % 86400.0) / 86400.0)
    return pd.DataFrame({"timestamp": timestamps, "y": values})


def _stub_source_with(monkeypatch: pytest.MonkeyPatch, responder: Any) -> StubSource:
    stub = StubSource()
    stub.respond(responder)
    monkeypatch.setattr(cli_module, "PromQLSource", lambda *_a, **_kw: stub)
    return stub


# ---------------------------------------------------------------------------
# Fix A: detect-once auto-widens for stratified detectors
# ---------------------------------------------------------------------------


def test_detect_once_window_auto_widens_for_stratified_detector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--window auto`` (the new default) must widen to the
    detector's preferred lookback. Pre-fix the fixed 1h default gave
    DayOfWeekMAD ~12 samples to score, the bucket baseline was empty,
    and the operator got a nonsense zero score."""
    stub = _stub_source_with(
        monkeypatch,
        lambda _: StubResponse(
            series=[QuerySeries(labels={"k": "v"}, samples=_diurnal_history(days=30))]
        ),
    )
    result = CliRunner().invoke(
        cli,
        [
            "detect-once",
            "--query",
            "rate(m[5m])",
            "--detector",
            "DayOfWeekMAD",
        ],
    )
    assert result.exit_code == 0, result.output
    # DayOfWeekMAD's default lookback is 4w == 2,419,200 seconds.
    assert stub.queries, "detect-once should issue exactly one range query"
    assert stub.queries[-1]["window"] == pytest.approx(4 * 7 * 86400.0)


def test_detect_once_explicit_window_overrides_auto(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit ``--window`` must win — operators who pass
    ``--window 1h`` deliberately are saying "I know better, use 1h"."""
    stub = _stub_source_with(
        monkeypatch,
        lambda _: StubResponse(
            series=[QuerySeries(labels={"k": "v"}, samples=_diurnal_history(days=30))]
        ),
    )
    result = CliRunner().invoke(
        cli,
        [
            "detect-once",
            "--query",
            "rate(m[5m])",
            "--detector",
            "DayOfWeekMAD",
            "--window",
            "1h",
        ],
    )
    assert result.exit_code == 0, result.output
    assert stub.queries[-1]["window"] == pytest.approx(3600.0)


def test_detect_once_auto_falls_back_to_hour_for_non_stratified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """For detectors with no ``query_window_seconds`` (MAD, Hampel,
    ...), ``--window auto`` must keep the historical 1h default."""
    stub = _stub_source_with(
        monkeypatch,
        lambda _: StubResponse(
            series=[QuerySeries(labels={"k": "v"}, samples=_diurnal_history(days=1))]
        ),
    )
    result = CliRunner().invoke(
        cli,
        [
            "detect-once",
            "--query",
            "rate(m[5m])",
            "--detector",
            "MAD",
        ],
    )
    assert result.exit_code == 0, result.output
    assert stub.queries[-1]["window"] == pytest.approx(3600.0)


# ---------------------------------------------------------------------------
# Fix B: inspect.py exception isolation via the shared helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_inspect_recovers_when_detector_query_window_raises(
    stub_source: StubSource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A buggy detector whose ``query_window_seconds`` raises must NOT
    crash the inspect endpoint — the shared
    :func:`query_window_seconds_for` helper swallows the exception
    and the inspect path falls back to the group default."""
    from typing import ClassVar

    from promanomaly import inspect as inspect_mod
    from promanomaly.detectors import registry

    class _BuggyDetector:
        name: ClassVar[str] = "_BuggyStratified"
        description: ClassVar[str] = "test stub"
        defaults: ClassVar[dict[str, Any]] = {}
        param_spec: ClassVar[tuple[Any, ...]] = ()

        @classmethod
        def query_window_seconds(cls, params: dict[str, Any]) -> float | None:
            raise RuntimeError("intentional bug")

        def fit_score(self, df: pd.DataFrame, freq: str, params: dict[str, Any]) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "timestamp": float(df["timestamp"].iloc[-1]),
                        "score": 0.5,
                        "baseline": 0.0,
                        "is_outside": False,
                    }
                ]
            )

    registry.register(_BuggyDetector)
    monkeypatch.setattr(
        registry, "_cache", {**registry.available(), "_BuggyStratified": _BuggyDetector}
    )

    try:
        stub_source.respond(
            lambda _: StubResponse(
                series=[QuerySeries(labels={"k": "v"}, samples=_diurnal_history(days=1))]
            )
        )
        raw = {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://stub"},
            "defaults": {"window": "15m", "step": "1m", "min_points": 5},
            "groups": [
                {
                    "name": "g",
                    "queries": [
                        {
                            "id": "q",
                            "promql": "rate(m[5m])",
                            "detectors": [{"name": "_BuggyStratified"}],
                        }
                    ],
                }
            ],
        }
        cfg = Config.model_validate(raw)
        # The endpoint must complete without raising despite the
        # detector's broken classmethod.
        payload = await inspect_mod.inspect_series(
            config=cfg,
            source=stub_source,
            store=SnapshotStore(),
            metric_id="q",
        )
        assert payload["id"] == "q"
        assert payload.get("status") != "error"
    finally:
        registry.unregister("_BuggyStratified")


# ---------------------------------------------------------------------------
# Fix D: HA suppression survives across replace_config
# ---------------------------------------------------------------------------


def test_reload_suppresses_misconfig_warning_when_ha_stays_enabled(
    stub_source: StubSource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reload that keeps HA enabled must keep suppressing the
    stratified warning — only the leader-acquisition callback should
    surface it. Boot-only coverage would miss a refactor that broke
    the reload-time gate."""
    calls: list[tuple[str, dict[str, Any]]] = []

    class _StubLogger:
        def warning(self, event: str, **kwargs: Any) -> None:
            calls.append((event, kwargs))

        def __getattr__(self, _name: str) -> Any:
            return lambda *_a, **_kw: None

    from promanomaly import runner as runner_mod

    monkeypatch.setattr(runner_mod, "logger", _StubLogger())

    raw_v1 = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"refresh_interval": "1m"},
        "safety": {"redis": {"url": "redis://localhost:6379/0"}},
        "highAvailability": {"enabled": True},
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
                                # Intentionally bad to trigger the warning.
                                "params": {"baseline_refresh_interval": "0s"},
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
    # Boot in HA mode: warning suppressed.
    assert not [e for e, _ in calls if e == "stratified_baseline_refresh_invalid"]

    # Reload while still in HA mode: warning must stay suppressed.
    runner.replace_config(Config.model_validate(raw_v1))
    assert not [e for e, _ in calls if e == "stratified_baseline_refresh_invalid"], (
        f"reload in HA mode must not log the warning; got: {[e for e, _ in calls]}"
    )


def test_reload_re_logs_warning_when_ha_disabled_on_reload(
    stub_source: StubSource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reload that turns HA off makes this process the sole one in
    the cluster; the stratified warning should fire so operators see
    the misconfig in this replica's logs."""
    calls: list[tuple[str, dict[str, Any]]] = []

    class _StubLogger:
        def warning(self, event: str, **kwargs: Any) -> None:
            calls.append((event, kwargs))

        def __getattr__(self, _name: str) -> Any:
            return lambda *_a, **_kw: None

    from promanomaly import runner as runner_mod

    monkeypatch.setattr(runner_mod, "logger", _StubLogger())

    raw_v1 = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "server": {"refresh_interval": "1m"},
        "safety": {"redis": {"url": "redis://localhost:6379/0"}},
        "highAvailability": {"enabled": True},
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
    runner = Runner(
        Config.model_validate(raw_v1), stub_source, SnapshotStore(), OperationalMetrics()
    )
    calls.clear()  # Drop any boot-time events from earlier paths.

    raw_v2 = dict(raw_v1)
    raw_v2["highAvailability"] = {"enabled": False}
    runner.replace_config(Config.model_validate(raw_v2))
    matches = [e for e, _ in calls if e == "stratified_baseline_refresh_invalid"]
    assert matches, (
        "reload that disables HA should surface the warning (this is now "
        f"the only process). Got events: {[e for e, _ in calls]}"
    )


# ---------------------------------------------------------------------------
# Fix E: backtest --inject multiplier capped at 2x for stratified
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backtest_inject_stratified_capped_at_2x_multiplier(
    stub_source: StubSource,
) -> None:
    """With a stratified detector and no ``--from``, the inject fetch
    must use the 2x cap rather than the 4x non-stratified multiplier.
    A 4w lookback at 4x = 16w would exceed typical TSDB retention."""
    stub_source.respond(
        lambda _: StubResponse(series=[QuerySeries(labels={}, samples=_diurnal_history(days=60))])
    )
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {"window": "1h", "step": "5m", "min_points": 5},
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
    await run_backtest(config=cfg, source=stub_source, group_name="g_strat", inject=True)
    max_window = max(call["window"] for call in stub_source.queries)
    # 4w * 2 = 8w. With the pre-fix 4x multiplier this would have been
    # 16w. Allow a small tolerance for the inject-mode history_seconds
    # logic but cap below the broken-4x ceiling.
    eight_weeks = 8 * 7 * 86400.0
    sixteen_weeks = 16 * 7 * 86400.0
    assert max_window == pytest.approx(eight_weeks), (
        f"expected the 2x stratified cap (8w); got {max_window} (16w == {sixteen_weeks})"
    )
