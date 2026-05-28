"""Regression tests for the round-6 stratified audit fixes.

Covers:

- ``detect-once --window auto --param lookback=2w`` respects the
  per-invocation lookback override. Pre-fix path was correct but
  untested; a future refactor that swapped the order of "merge
  params" and "resolve window" would silently regress.
- ``backtest --from`` overrides the 2x stratified multiplier. The
  round-5 fix gated the multiplier on ``inject and start is None``
  but only covered the ``start is None`` half in tests; this pins
  the other half so a refactor of the conditional gets caught.
- ``_query_has_stratified_detector`` is exercised directly: known
  stratified detector → True, non-stratified → False, unknown name
  → False (no crash), buggy ``query_window_seconds`` → False.
- The structured ``stratified_baseline_fetch_failed`` event fires
  when the baseline fetch raises with a populated cache so
  operators can distinguish "fail-loud-with-stale-cache-present"
  from a cold-start failure.
"""

from __future__ import annotations

from typing import Any, ClassVar

import numpy as np
import pandas as pd
import pytest
from click.testing import CliRunner

from promanomaly import cli as cli_module
from promanomaly.backtest import run_backtest
from promanomaly.cli import cli
from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.detectors import registry
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.source import QuerySeries, SourceQueryError
from promanomaly.state import SnapshotStore
from promanomaly.stratified_fetcher import _query_has_stratified_detector
from tests.conftest import StubResponse, StubSource, make_series


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
# Fix A: detect-once --window auto honours --param lookback overrides
# ---------------------------------------------------------------------------


def test_detect_once_window_auto_honours_param_lookback_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--param lookback=2w`` must shrink the auto-widened fetch to
    2 weeks rather than the detector's default 4w. Without this,
    operators couldn't iterate on smaller windows for ad-hoc debugging
    of a stratified detector against a TSDB with limited retention."""
    stub = _stub_source_with(
        monkeypatch,
        lambda _: StubResponse(
            series=[QuerySeries(labels={"k": "v"}, samples=_diurnal_history(days=20))]
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
            "--param",
            "lookback=2w",
        ],
    )
    assert result.exit_code == 0, result.output
    # 2w * 86400 == 1,209,600 seconds.
    assert stub.queries[-1]["window"] == pytest.approx(2 * 7 * 86400.0)


# ---------------------------------------------------------------------------
# Fix B: backtest --from overrides the 2x stratified multiplier
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backtest_inject_with_from_skips_stratified_multiplier(
    stub_source: StubSource,
) -> None:
    """When ``--from`` is set (start is not None), the inject fetch
    uses the operator-supplied range exactly, not the 2x stratified
    multiplier on top. Operators bounding the fetch this way trust
    the cli to honour their bound."""
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
    end_ts = 1_700_000_000.0
    start_ts = end_ts - 3 * 7 * 86400.0  # 3-week range
    await run_backtest(
        config=cfg,
        source=stub_source,
        group_name="g_strat",
        inject=True,
        end=end_ts,
        start=start_ts,
    )
    max_window = max(call["window"] for call in stub_source.queries)
    # With --from the multiplier is 1x: the fetch uses
    # max(history_seconds=3w, override=4w) == 4w. Without the gate
    # it would have been 8w.
    assert max_window == pytest.approx(4 * 7 * 86400.0)
    assert max_window < 8 * 7 * 86400.0  # explicit guard against regression


# ---------------------------------------------------------------------------
# Fix G: _query_has_stratified_detector behaves correctly across cases
# ---------------------------------------------------------------------------


class _BuggyStratifiedDetector:
    """Test detector whose ``query_window_seconds`` raises."""

    name: ClassVar[str] = "_BuggyStratifiedForRound6"
    description: ClassVar[str] = "test stub"
    defaults: ClassVar[dict[str, Any]] = {}
    param_spec: ClassVar[tuple[Any, ...]] = ()

    @classmethod
    def query_window_seconds(cls, params: dict[str, Any]) -> float | None:
        raise RuntimeError("intentional bug")

    @classmethod
    def baseline_refresh_interval_seconds(cls, params: dict[str, Any]) -> float | None:
        return 3600.0

    def fit_score(self, df: pd.DataFrame, freq: str, params: dict[str, Any]) -> pd.DataFrame:
        return pd.DataFrame(
            [{"timestamp": 0.0, "score": 0.0, "baseline": 0.0, "is_outside": False}]
        )


def test_query_has_stratified_detector_recognises_stratified_class() -> None:
    from promanomaly.config import DetectorEntry

    entry = DetectorEntry(name="DayOfWeekMAD")
    assert _query_has_stratified_detector(entry) is True


def test_query_has_stratified_detector_rejects_non_stratified_class() -> None:
    from promanomaly.config import DetectorEntry

    entry = DetectorEntry(name="MAD")
    assert _query_has_stratified_detector(entry) is False


def test_query_has_stratified_detector_handles_unknown_name() -> None:
    from promanomaly.config import DetectorEntry

    entry = DetectorEntry(name="DefinitelyNotARegisteredDetector")
    # The registry would raise UnknownDetectorError; the helper must
    # swallow it and report False rather than propagate. The runner's
    # standard detector-lookup path surfaces the unknown-detector
    # error at scoring time with full context.
    assert _query_has_stratified_detector(entry) is False


def test_query_has_stratified_detector_handles_buggy_classmethod() -> None:
    """A detector that exposes both classmethods but raises from one
    must be treated as non-stratified — same behaviour the shared
    helpers' exception swallowing produces in the fetch path."""
    from promanomaly.config import DetectorEntry

    registry.register(_BuggyStratifiedDetector)
    try:
        entry = DetectorEntry(name="_BuggyStratifiedForRound6")
        # query_window_seconds raises → query_window_seconds_for returns None
        # → helper returns False.
        assert _query_has_stratified_detector(entry) is False
    finally:
        registry.unregister("_BuggyStratifiedForRound6")


# ---------------------------------------------------------------------------
# Fix D: stratified_baseline_fetch_failed log event distinguishes cache state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_baseline_fetch_failure_with_stale_cache_logs_distinct_event(
    stub_source: StubSource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the baseline fetch fails AND a stale cache entry exists,
    the runner logs ``stratified_baseline_fetch_failed`` with
    ``cache_present=True`` so operators can tell "TSDB blip but we
    have history we're refusing to use" from a cold-start failure.

    The exception still propagates per the fail-loud semantics
    documented in scaling-stratified.md; the log event is purely
    additional context.
    """
    events: list[tuple[str, dict[str, Any]]] = []

    class _StubLogger:
        def warning(self, event: str, **kwargs: Any) -> None:
            events.append((event, kwargs))

        def info(self, event: str, **kwargs: Any) -> None:
            events.append((event, kwargs))

        def __getattr__(self, _name: str) -> Any:
            return lambda *_a, **_kw: None

    from promanomaly import stratified_fetcher as fetcher_mod

    # The warning fires from the StratifiedFetcher since the cache +
    # fetch lifecycle live there now; patching the runner's logger
    # would no longer catch it.
    monkeypatch.setattr(fetcher_mod, "logger", _StubLogger())

    history = _diurnal_history(days=30)
    series = make_series({"k": "v"}, history)
    state = {"fail_baseline": False}

    def responder(_: str) -> StubResponse:
        if state["fail_baseline"]:
            return StubResponse(error=SourceQueryError("server_error", "HTTP 500"))
        return StubResponse(series=[series])

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
    runner = Runner(cfg, stub_source, SnapshotStore(), OperationalMetrics())

    # First run primes the cache.
    await runner.run_group("g")
    assert ("g", "weekly") in runner.stratified_cache_keys_for_testing()

    # Age the cache so the next run forces a baseline re-fetch, then
    # arrange the responder to fail. The short fetch happens first
    # (and succeeds against the responder's default branch); the
    # baseline fetch is what trips because we'll flip the flag once
    # the short fetch is done. We can't easily intercept between the
    # two with the StubSource, so the simplest setup that exercises
    # the desired path is to make ALL queries fail and just age the
    # cache: the short fetch fails first, then the test asserts on
    # the log surface.
    runner.age_stratified_cache_entry_for_testing(("g", "weekly"), 2 * 3600.0)
    state["fail_baseline"] = True

    events.clear()
    await runner.run_group("g")

    matches = [e for e, _ in events if e == "stratified_baseline_fetch_failed"]
    # The log event is informational. It fires once whenever the
    # stratified-fetch path raises with a cache entry present.
    assert matches, f"expected stratified_baseline_fetch_failed; got: {[e for e, _ in events]}"
