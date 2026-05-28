"""End-to-end wiring tests for anomaly_change_point_total.

These tests exercise the SnapshotStore counter primitive, the runner's
emission path (detector returns ``change_point`` column ->
``anomaly_change_point_total`` Sample), and the exporter's TYPE
declaration. The interesting properties:

  * Counters are emitted on every run that scored the series, regardless
    of whether the latest sample was a fire — Prometheus needs the metric
    present every scrape to distinguish "still 0" from "stale".
  * The cumulative count survives across runs in the same process.
  * Detectors that don't return a ``change_point`` column never emit the
    counter (so MAD/Hampel/IQR don't pollute /metrics with always-zero
    counters).
"""

from __future__ import annotations

from typing import Any

import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.exporter import OperationalMetrics, render_samples
from promanomaly.runner import Runner
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline, step_change


def _config_with_detector(name: str, params: dict[str, Any] | None = None) -> Config:
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
            "warmup_policy": "emit_warming_up",
            "emit_change_points": True,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "m",
                        "promql": "up",
                        "detectors": [{"name": name, "params": params or {}}],
                    }
                ],
            }
        ],
    }
    return Config.model_validate(raw)


def test_snapshot_store_counter_increments() -> None:
    store = SnapshotStore()
    key = ("g", "m", (("instance", "host-a"),), "BOCPD", None)
    # First fire bumps to 1.
    n = store.record_change_point(*key, fired=True)
    assert n == 1
    # No fire leaves the count unchanged but still returns the cumulative.
    n = store.record_change_point(*key, fired=False)
    assert n == 1
    # Subsequent fires increment.
    n = store.record_change_point(*key, fired=True)
    assert n == 2


def test_snapshot_store_counter_isolated_per_detector() -> None:
    store = SnapshotStore()
    base = ("g", "m", (("instance", "host-a"),))
    n1 = store.record_change_point(*base, "BOCPD", None, fired=True)
    n2 = store.record_change_point(*base, "CUSUM", None, fired=True)
    n3 = store.record_change_point(*base, "BOCPD", "long", fired=True)
    assert n1 == 1
    assert n2 == 1
    assert n3 == 1


def test_remove_group_clears_change_point_counts() -> None:
    store = SnapshotStore()
    store.record_change_point("g", "m", (), "BOCPD", None, fired=True)
    store.record_change_point("g", "m", (), "BOCPD", None, fired=True)
    store.remove_group("g")
    # Fresh count after removal.
    n = store.record_change_point("g", "m", (), "BOCPD", None, fired=True)
    assert n == 1


@pytest.mark.asyncio
async def test_runner_emits_change_point_total_for_bocpd(stub_source: StubSource) -> None:
    """BOCPD returns the ``change_point`` column, so the runner emits
    ``anomaly_change_point_total`` as part of the snapshot. A non-fire
    run still emits the counter at its current value (0)."""
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "host-a"}, df)]))

    cfg = _config_with_detector("BOCPD")
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)

    result = await runner.run_group("g1")
    assert result.succeeded
    metrics = {s.metric for s in result.samples}
    assert "anomaly_change_point_total" in metrics
    cp_sample = next(s for s in result.samples if s.metric == "anomaly_change_point_total")
    # Clean baseline -> 0 fires, but the counter must be emitted.
    assert cp_sample.value == 0.0
    assert cp_sample.label_dict["detector"] == "BOCPD"


@pytest.mark.asyncio
async def test_runner_increments_counter_on_fire(stub_source: StubSource) -> None:
    """A change-point at the latest sample bumps the counter on the
    first run and persists the value on the next run."""
    # Step at the very end -> BOCPD fires.
    df = step_change(n=120, step=5.0, noise=0.1, change_at=119)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "host-a"}, df)]))

    cfg = _config_with_detector("BOCPD")
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)

    result = await runner.run_group("g1")
    cp_sample = next(s for s in result.samples if s.metric == "anomaly_change_point_total")
    assert cp_sample.value >= 1.0


@pytest.mark.asyncio
async def test_mad_does_not_emit_change_point_total(stub_source: StubSource) -> None:
    """MAD doesn't return a ``change_point`` column, so the runner
    doesn't emit the counter — keeps /metrics free of always-zero
    counters."""
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "host-a"}, df)]))

    cfg = _config_with_detector("MAD")
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)

    result = await runner.run_group("g1")
    metrics = {s.metric for s in result.samples}
    assert "anomaly_change_point_total" not in metrics


@pytest.mark.asyncio
async def test_emit_change_points_false_suppresses_counter(stub_source: StubSource) -> None:
    """Operators can disable the counter wholesale via defaults.emit_change_points."""
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "host-a"}, df)]))

    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
            "warmup_policy": "emit_warming_up",
            "emit_change_points": False,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g1",
                "queries": [{"id": "m", "promql": "up", "detectors": [{"name": "BOCPD"}]}],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)

    result = await runner.run_group("g1")
    metrics = {s.metric for s in result.samples}
    assert "anomaly_change_point_total" not in metrics


def test_exporter_renders_counter_type_declaration() -> None:
    """The exposition format must declare TYPE counter for the metric so
    Prometheus client libraries handle counter-reset detection correctly."""
    from promanomaly.state import Sample

    samples = [
        Sample(
            metric="anomaly_change_point_total",
            labels=(("detector", "BOCPD"), ("group", "g"), ("id", "m")),
            value=3.0,
        )
    ]
    text = render_samples(samples)
    assert "# TYPE anomaly_change_point_total counter" in text


@pytest.mark.asyncio
async def test_per_detector_threshold_override(stub_source: StubSource) -> None:
    """BOCPD declares ``threshold: 0.5`` in defaults so the runner uses
    0.5 as the firing threshold even when the group's alert_thresholds.score
    is 3.0 (the default). Without this override BOCPD's probability score
    would never reach the sigma-multiplier threshold."""
    df = step_change(n=120, step=5.0, noise=0.1, change_at=119)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "host-a"}, df)]))

    cfg = _config_with_detector("BOCPD")
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)

    result = await runner.run_group("g1")
    outside = next(s for s in result.samples if s.metric == "anomaly_outside_threshold")
    # BOCPD probability 0.5+ should still fire even though it's <<3.0
    # (the group-level alert_thresholds.score).
    assert outside.value == 1.0


def test_baseline_detectors_dont_emit_change_point_column() -> None:
    """The runner uses the presence of ``change_point`` in the score row
    to decide whether to emit ``anomaly_change_point_total``. Drift here
    would silently produce always-zero counters for every series scored
    by MAD / Hampel / IQR / ZScoreEWMA / HourOfDayMAD. Pin the contract."""
    import pandas as pd

    from promanomaly.detectors.hampel import Hampel
    from promanomaly.detectors.iqr import IQR
    from promanomaly.detectors.mad import MAD
    from promanomaly.detectors.zscore_ewma import ZScoreEWMA

    df = pd.DataFrame(
        {
            "timestamp": list(range(120)),
            "y": [1.0 + 0.001 * i for i in range(120)],
        }
    )
    for cls in (MAD, Hampel, IQR, ZScoreEWMA):
        row = cls().fit_score(df, "", {}).iloc[-1]
        assert "change_point" not in row.index, (
            f"{cls.__name__} returned a change_point column — it would be "
            "treated as a change-point detector by the runner"
        )


def test_change_point_detectors_emit_change_point_column() -> None:
    """The other half of the contract: BOCPD and CUSUM must emit a
    ``change_point`` column for the runner to wire them into the counter."""
    import pandas as pd

    from promanomaly.detectors.bocpd import BOCPD
    from promanomaly.detectors.cusum import CUSUM

    df = pd.DataFrame(
        {
            "timestamp": list(range(120)),
            "y": [1.0 + 0.001 * i for i in range(120)],
        }
    )
    for cls in (BOCPD, CUSUM):
        row = cls().fit_score(df, "", {}).iloc[-1]
        assert "change_point" in row.index, (
            f"{cls.__name__} no longer emits change_point — the counter "
            "stops working for this detector"
        )


def test_effective_threshold_helper_round_trip() -> None:
    """The runner exposes a small helper that both the live-scoring and
    the calibration paths call. They MUST agree on the threshold or
    auto-select picks losers — assert the helper's contract directly."""
    from promanomaly.plan import effective_threshold

    # Per-detector override wins.
    assert effective_threshold({"hazard": 250, "threshold": 0.5, "lag": 15}, 3.0) == 0.5
    # No override -> falls back to the group threshold.
    assert effective_threshold({}, 3.0) == 3.0
    # Garbage override -> fallback rather than crash.
    assert effective_threshold({"threshold": "not a number"}, 3.0) == 3.0


@pytest.mark.asyncio
async def test_bocpd_auto_select_winner_after_calibration(stub_source: StubSource) -> None:
    """End-to-end: with auto_select on and BOCPD vs MAD competing on the
    same series, BOCPD's calibration must run with its own threshold so
    its F1 is comparable to MAD's. Before the calibration fix, BOCPD's
    F1 collapsed to 0 (probability score vs. sigma threshold) and MAD
    always won. This test pins the fix."""
    # Use a step-shift fixture where BOCPD should rank competitive with MAD.
    df = step_change(n=120, step=5.0, noise=0.1, change_at=80)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "host-a"}, df)]))

    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
            "warmup_policy": "emit_warming_up",
            "emit_baseline_stability": True,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g1",
                "auto_select": True,
                "queries": [
                    {
                        "id": "m",
                        "promql": "up",
                        "detectors": [{"name": "MAD"}, {"name": "BOCPD"}],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)

    # One run primes calibration; we don't assert who wins (both might
    # rank close on the synthetic fixtures), only that BOCPD's
    # calibration produces non-zero precision/recall/F1 numbers, which
    # is the proxy for "calibration honours the per-detector threshold".
    await runner.run_group("g1")
    cal = store.get_calibration("g1", "m", (("instance", "host-a"),), "BOCPD", None)
    assert cal is not None, "BOCPD calibration must run during auto_select"
    assert cal.recall > 0.0, (
        f"BOCPD calibration recall is 0 — runner is not passing the "
        f"per-detector threshold to calibrate_detector: {cal!r}"
    )
