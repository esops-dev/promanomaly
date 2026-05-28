"""Cohort detector tests.

Covers the detector's per-series math in isolation and the runner-level
integration where the cohort context is pre-computed across members.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.detectors.cohort import (
    _PARAM_COHORT_MAD,
    _PARAM_COHORT_MEDIAN,
    _PARAM_COHORT_SIZE,
    Cohort,
    cohort_key_for,
    compute_cohort_baseline,
    is_cohort_aware,
)
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series


def _window(values: list[float]) -> pd.DataFrame:
    """Build a fixed-step frame so the detector can score the latest sample."""
    return pd.DataFrame({"timestamp": np.arange(len(values), dtype=float), "y": values})


# ---------------------------------------------------------------------------
# Detector unit behaviour
# ---------------------------------------------------------------------------


def test_metadata_exposes_cohort_aware_flag() -> None:
    assert Cohort.name == "Cohort"
    assert is_cohort_aware(Cohort)
    assert Cohort.defaults == {"cohort_label": "instance", "min_cohort_size": 5}


def test_param_spec_documents_every_default() -> None:
    spec_defaults = {p.name: p.default for p in Cohort.param_spec}
    assert spec_defaults == Cohort.defaults
    for spec in Cohort.param_spec:
        assert spec.doc.strip()
        assert spec.type_.strip()


def test_score_zero_when_cohort_too_small() -> None:
    # No baseline injected -> the runner would have skipped this cohort
    # because it didn't meet min_cohort_size. The detector emits a zero
    # row so the loop produces a defined, but non-firing, sample.
    detector = Cohort()
    row = detector.fit_score(_window([10.0, 11.0, 10.5]), "", {}).iloc[-1]
    assert row["score"] == 0.0
    assert bool(row["is_outside"]) is False


def test_score_high_when_member_deviates_from_cohort() -> None:
    detector = Cohort()
    df = _window([15.0, 15.5, 14.8, 15.1, 15.3])
    # Inject a cohort baseline that this series is far from: cohort
    # median 10, MAD 1 -> the series's rolling mean (~15.14) is 5 MADs
    # away, well above the default alert_thresholds.score=3.
    row = detector.fit_score(
        df,
        "",
        {
            _PARAM_COHORT_MEDIAN: 10.0,
            _PARAM_COHORT_MAD: 1.0,
            _PARAM_COHORT_SIZE: 8,
            "min_cohort_size": 5,
        },
    ).iloc[-1]
    assert float(row["score"]) > 3.0
    assert float(row["baseline"]) == pytest.approx(10.0)


def test_score_zero_on_constant_fleet() -> None:
    # MAD=0 means every member is at the same value; the detector
    # collapses the score to zero rather than dividing by zero.
    detector = Cohort()
    row = detector.fit_score(
        _window([10.0, 10.0, 10.0]),
        "",
        {
            _PARAM_COHORT_MEDIAN: 10.0,
            _PARAM_COHORT_MAD: 0.0,
            _PARAM_COHORT_SIZE: 6,
            "min_cohort_size": 5,
        },
    ).iloc[-1]
    assert row["score"] == 0.0


# ---------------------------------------------------------------------------
# cohort_key helper
# ---------------------------------------------------------------------------


def test_cohort_key_strips_cohort_label_only() -> None:
    labels = {"job": "node", "instance": "node-7", "zone": "eu"}
    assert cohort_key_for(labels, "instance") == (("job", "node"), ("zone", "eu"))


def test_cohort_key_ignores_missing_cohort_label() -> None:
    # A series that lacks the cohort label still has a stable key —
    # falls into a single-member cohort, which the runner then drops
    # because it cannot meet ``min_cohort_size``.
    labels = {"job": "node"}
    assert cohort_key_for(labels, "instance") == (("job", "node"),)


# ---------------------------------------------------------------------------
# compute_cohort_baseline
# ---------------------------------------------------------------------------


def test_compute_baseline_median_and_mad_are_stable() -> None:
    cohort = [
        np.array([10.0, 10.0, 10.0]),
        np.array([10.5, 10.0, 9.5]),
        np.array([9.0, 9.5, 10.0]),
        np.array([11.0, 11.0, 11.0]),
        np.array([10.0, 10.5, 10.0]),
    ]
    out = compute_cohort_baseline(cohort)
    assert out is not None
    median, mad = out
    # At each step the median is around 10; median over time is ~10.
    assert median == pytest.approx(10.0, abs=0.5)
    # MAD across this cohort is small but non-zero.
    assert mad >= 0.0


def test_compute_baseline_returns_none_on_empty_input() -> None:
    assert compute_cohort_baseline([]) is None


def test_compute_baseline_trims_to_shortest_member() -> None:
    cohort = [
        np.array([10.0, 10.5, 10.2]),
        np.array([10.0]),  # only the most recent timestamp survives
        np.array([10.0, 9.5]),
    ]
    out = compute_cohort_baseline(cohort)
    assert out is not None
    median, _mad = out
    assert median == pytest.approx(10.0, abs=0.5)


# ---------------------------------------------------------------------------
# Runner integration
# ---------------------------------------------------------------------------


def _config(detectors: list[dict[str, Any]] | None = None) -> Config:
    detectors = detectors or [{"name": "Cohort"}]
    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
            "alert_thresholds": {"score": 3.0},
            # Disable baseline stability so the calibration cycle (which
            # is rolling-window-shaped, not cohort-shaped) doesn't run
            # and slow the integration test.
            "emit_baseline_stability": False,
        },
        "groups": [
            {
                "name": "fleet",
                "queries": [
                    {
                        "id": "cpu_busy",
                        "promql": "node_cpu_busy",
                        "detectors": detectors,
                    }
                ],
            }
        ],
    }
    return Config.model_validate(raw)


def _fleet_response(divergent_member: str) -> StubResponse:
    """Five clean members + one outlier (``divergent_member``).

    Each member's window is short to keep the test fast; the runner's
    ``min_points`` is set low enough to bypass warm-up.
    """
    n = 40
    members = [
        ("node-a", 10.0),
        ("node-b", 10.1),
        ("node-c", 9.9),
        ("node-d", 10.05),
        ("node-e", 9.95),
        (divergent_member, 30.0),
    ]
    series_list = []
    for name, mean in members:
        rng = np.random.default_rng(hash(name) & 0xFFFFFFFF)
        values = mean + rng.normal(0.0, 0.05, size=n)
        df = pd.DataFrame({"timestamp": np.arange(n, dtype=float) * 15.0, "y": values})
        series_list.append(make_series({"job": "node", "instance": name}, df))
    return StubResponse(series=series_list)


@pytest.mark.asyncio
async def test_runner_emits_high_score_for_divergent_cohort_member(
    stub_source: StubSource,
) -> None:
    stub_source.respond(lambda _: _fleet_response("node-bad"))
    cfg = _config()
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)

    result = await runner.run_group("fleet")
    assert result.succeeded
    samples = {
        (dict(s.labels)["instance"], s.metric): s
        for s in result.samples
        if "instance" in dict(s.labels)
    }
    # Divergent member fires.
    bad_score = float(samples[("node-bad", "anomaly_score")].value)
    assert bad_score > 3.0
    assert float(samples[("node-bad", "anomaly_outside_threshold")].value) == 1.0
    # Clean members do not fire (small allowance for cohort MAD scaling).
    clean_score = float(samples[("node-a", "anomaly_score")].value)
    assert clean_score < 3.0
    # cohort_label opt-in label appears on every Cohort sample.
    bad_labels = dict(samples[("node-bad", "anomaly_score")].labels)
    assert bad_labels["cohort_label"] == "instance"
    assert bad_labels["detector"] == "Cohort"


@pytest.mark.asyncio
async def test_runner_skips_calibration_for_cohort_detector(
    stub_source: StubSource,
) -> None:
    # Calibration uses a synthetic-injection cycle on a single series and
    # has no cohort context to inject. Without the runner skip, Cohort
    # would score 0 in calibration -> F1=0 -> confidence_score=0
    # published on /metrics, which is actively misleading. Pin the skip:
    # no confidence / baseline-stability samples should appear in the run,
    # even with the defaults that would otherwise trigger calibration.
    stub_source.respond(lambda _: _fleet_response("node-bad"))
    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
            "alert_thresholds": {"score": 3.0},
            # Defaults: baseline-stability ON -> calibration would
            # normally fire on the first scoring pass.
            "emit_baseline_stability": True,
        },
        "groups": [
            {
                "name": "fleet",
                "queries": [
                    {
                        "id": "cpu_busy",
                        "promql": "node_cpu_busy",
                        "detectors": [{"name": "Cohort"}],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)

    result = await runner.run_group("fleet")
    assert result.succeeded
    metrics = {s.metric for s in result.samples}
    # Neither calibration-driven gauge should appear for a Cohort plan.
    assert "anomaly_confidence_score" not in metrics
    assert "anomaly_baseline_stability" not in metrics


def test_cohort_with_auto_select_true_logs_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Pin the boot warning: putting Cohort in a group with
    # auto_select: true silently suppresses its output, so we surface
    # a one-time log at runner construction. Operators can grep for
    # cohort_with_auto_select_true in logs as a config-health check.
    # Capture via monkeypatch on the runner module's logger because
    # structlog's caplog bridge depends on the wrapper-factory routing
    # configured in main.py — the existing stratified-warning tests use
    # the same approach for the same reason.
    from promanomaly import runner as runner_mod

    calls: list[tuple[str, dict[str, Any]]] = []

    class _Spy:
        def warning(self, event: str, **kwargs: Any) -> None:
            calls.append((event, kwargs))

        def info(self, *_a: Any, **_kw: Any) -> None:
            pass

        def error(self, *_a: Any, **_kw: Any) -> None:
            pass

        def exception(self, *_a: Any, **_kw: Any) -> None:
            pass

    monkeypatch.setattr(runner_mod, "logger", _Spy())

    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "groups": [
            {
                "name": "fleet",
                "auto_select": True,
                "queries": [
                    {
                        "id": "cpu_busy",
                        "promql": "up",
                        "detectors": [{"name": "Cohort"}, {"name": "MAD"}],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    Runner(cfg, StubSource(), SnapshotStore(), OperationalMetrics())
    surfaced = [event for event, _ in calls if event == "cohort_with_auto_select_true"]
    assert surfaced, f"expected cohort_with_auto_select_true; got {[e for e, _ in calls]}"


def test_score_zero_when_member_diverges_from_constant_fleet() -> None:
    # Documented edge case: cohort MAD = 0 collapses the score to zero
    # even when the member clearly diverges. Pin the behaviour so a
    # future "fix" that flips the sign without documentation is a loud
    # regression rather than a silent semantic change.
    detector = Cohort()
    row = detector.fit_score(
        _window([42.0, 42.0, 42.0]),
        "",
        {
            _PARAM_COHORT_MEDIAN: 10.0,
            _PARAM_COHORT_MAD: 0.0,
            _PARAM_COHORT_SIZE: 6,
            "min_cohort_size": 5,
        },
    ).iloc[-1]
    assert row["score"] == 0.0
    assert float(row["baseline"]) == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_runner_includes_cohort_in_ensemble_fusion(
    stub_source: StubSource,
) -> None:
    # Pin that Cohort's outcome participates in ensemble fusion even
    # though it doesn't have a calibration record. The composite gauge
    # should fire when the cohort vote crosses the threshold, regardless
    # of whether single-series detectors agree.
    stub_source.respond(lambda _: _fleet_response("node-bad"))
    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
            "alert_thresholds": {"score": 3.0},
            "emit_baseline_stability": False,
        },
        "groups": [
            {
                "name": "fleet",
                "ensemble": {"method": "max"},
                "queries": [
                    {
                        "id": "cpu_busy",
                        "promql": "node_cpu_busy",
                        "detectors": [{"name": "Cohort"}],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)

    result = await runner.run_group("fleet")
    assert result.succeeded
    composite_outside = [
        s for s in result.samples if s.metric == "anomaly_composite_outside_threshold"
    ]
    # One composite sample per cohort member.
    fires = [s for s in composite_outside if s.value == 1.0]
    fire_instances = {dict(s.labels).get("instance") for s in fires}
    assert "node-bad" in fire_instances


@pytest.mark.asyncio
async def test_runner_skips_cohort_below_min_size(
    stub_source: StubSource,
) -> None:
    # Only two cohort members -> below default min_cohort_size=5 ->
    # detector receives no injected baseline and emits a zero-score row.
    n = 40
    series_list = [
        make_series(
            {"job": "node", "instance": "node-a"},
            pd.DataFrame({"timestamp": np.arange(n, dtype=float), "y": np.full(n, 10.0)}),
        ),
        make_series(
            {"job": "node", "instance": "node-b"},
            pd.DataFrame({"timestamp": np.arange(n, dtype=float), "y": np.full(n, 30.0)}),
        ),
    ]
    stub_source.respond(lambda _: StubResponse(series=series_list))
    cfg = _config()
    store = SnapshotStore()
    ops = OperationalMetrics()
    runner = Runner(cfg, stub_source, store, ops)

    result = await runner.run_group("fleet")
    assert result.succeeded
    for s in result.samples:
        if s.metric == "anomaly_outside_threshold":
            # No member fires because the cohort is too small to be scored.
            assert s.value == 0.0
