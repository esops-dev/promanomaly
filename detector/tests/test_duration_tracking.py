"""``anomaly_duration_seconds`` semantics.

The duration counter tracks wall-clock seconds since
``anomaly_outside_threshold`` last transitioned 0 -> 1 for a given
label set. It resets to 0 on the next run where the threshold is clear
and is rebuilt-from-zero on restart (the snapshot store is in-memory by
design).

These tests pin the behaviour at the snapshot-store layer where it
lives — the runner integration test already proves the metric flows
through end-to-end.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pandas as pd
import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.detectors import registry
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline


def _start(
    store: SnapshotStore,
    *,
    detector: str = "MAD",
    instance: str | None = None,
    outside: bool,
    now: float,
    series_key: tuple[tuple[str, str], ...] = (),
) -> float:
    return store.update_duration(
        "g",
        "q",
        series_key,
        detector_name=detector,
        detector_instance=instance,
        outside=outside,
        now=now,
    )


def test_duration_starts_at_zero_on_first_transition() -> None:
    store = SnapshotStore()
    assert _start(store, outside=True, now=100.0) == 0.0


def test_duration_accumulates_while_outside() -> None:
    store = SnapshotStore()
    _start(store, outside=True, now=100.0)
    assert _start(store, outside=True, now=137.5) == pytest.approx(37.5)


def test_duration_resets_to_zero_when_threshold_clears() -> None:
    store = SnapshotStore()
    _start(store, outside=True, now=100.0)
    _start(store, outside=True, now=200.0)
    assert _start(store, outside=False, now=300.0) == 0.0
    # Next transition starts a fresh counter.
    assert _start(store, outside=True, now=400.0) == 0.0


def test_duration_keyed_by_series() -> None:
    store = SnapshotStore()
    a = (("instance", "host-a"),)
    b = (("instance", "host-b"),)
    _start(store, outside=True, now=100.0, series_key=a)
    # b is independently tracked: no accumulated duration.
    assert _start(store, outside=True, now=200.0, series_key=b) == 0.0
    assert _start(store, outside=True, now=250.0, series_key=a) == pytest.approx(150.0)


def test_duration_keyed_by_detector() -> None:
    """Critical: a non-firing detector must not reset a firing
    sibling's counter. Without per-detector keying, MAD fires at t=0
    and Hampel-not-firing at t=10 pops the start, breaking MAD's
    gauge."""
    store = SnapshotStore()
    _start(store, detector="MAD", outside=True, now=0.0)
    # Hampel doesn't fire — its update must not touch MAD's timer.
    assert _start(store, detector="Hampel", outside=False, now=10.0) == 0.0
    # MAD's accumulated duration is unbroken.
    assert _start(store, detector="MAD", outside=True, now=60.0) == pytest.approx(60.0)


def test_duration_keyed_by_detector_instance() -> None:
    """Multi-instance detectors (e.g. MAD@short / MAD@long) get
    independent counters."""
    store = SnapshotStore()
    _start(store, detector="MAD", instance="short", outside=True, now=0.0)
    assert _start(store, detector="MAD", instance="long", outside=True, now=30.0) == 0.0
    assert _start(store, detector="MAD", instance="short", outside=True, now=60.0) == pytest.approx(
        60.0
    )


def test_duration_cleared_when_group_removed() -> None:
    store = SnapshotStore()
    _start(store, outside=True, now=100.0)
    store.remove_group("g")
    # New group entry starts fresh.
    assert _start(store, outside=True, now=200.0) == 0.0


# ---------------------------------------------------------------------------
# Integration with the runner / exporter contract
# ---------------------------------------------------------------------------


class _LowScore:
    """Detector double that never crosses the threshold."""

    name: ClassVar[str] = "Quiet"
    description: ClassVar[str] = "always under threshold"
    defaults: ClassVar[dict[str, Any]] = {}

    def fit_score(self, df: pd.DataFrame, freq: str, params: dict[str, Any]) -> pd.DataFrame:
        del freq, params
        return pd.DataFrame(
            [
                {
                    "timestamp": float(df["timestamp"].iloc[-1]),
                    "score": 0.1,
                    "baseline": 0.0,
                    "is_outside": False,
                }
            ]
        )


def _config(*, emit_duration: bool = True) -> Config:
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
            "warmup_policy": "emit_warming_up",
            "emit_baseline_stability": False,
            "emit_duration": emit_duration,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "m",
                        "promql": "up",
                        "detectors": [{"name": "Quiet"}],
                    }
                ],
            }
        ],
    }
    return Config.model_validate(raw)


@pytest.fixture
def _register_quiet() -> Any:
    registry.register(_LowScore)  # type: ignore[arg-type]
    try:
        yield
    finally:
        registry.unregister("Quiet")


@pytest.mark.asyncio
async def test_duration_emitted_when_enabled(
    stub_source: StubSource,
    _register_quiet: Any,
) -> None:
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "h"}, df)]))

    cfg = _config(emit_duration=True)
    runner = Runner(cfg, stub_source, SnapshotStore(), OperationalMetrics())
    result = await runner.run_group("g1")
    metrics = {s.metric for s in result.samples}
    assert "anomaly_duration_seconds" in metrics
    duration = next(s for s in result.samples if s.metric == "anomaly_duration_seconds")
    # The detector never fires, so the duration counter sits at zero.
    assert duration.value == 0.0


@pytest.mark.asyncio
async def test_duration_suppressed_when_disabled(
    stub_source: StubSource,
    _register_quiet: Any,
) -> None:
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({}, df)]))

    cfg = _config(emit_duration=False)
    runner = Runner(cfg, stub_source, SnapshotStore(), OperationalMetrics())
    result = await runner.run_group("g1")
    metrics = {s.metric for s in result.samples}
    assert "anomaly_duration_seconds" not in metrics
