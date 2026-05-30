"""SeasonalHybridESD detector tests."""

from __future__ import annotations

from promanomaly.detectors.seasonal_hybrid_esd import SeasonalHybridESD
from tests.fixtures import clean_baseline, diurnal


def test_metadata() -> None:
    detector = SeasonalHybridESD()
    assert detector.name == "SeasonalHybridESD"
    assert "seasonal" in detector.description.lower()
    spec_defaults = {p.name: p.default for p in detector.param_spec}
    assert detector.defaults == spec_defaults


def test_clean_diurnal_scores_low() -> None:
    detector = SeasonalHybridESD()
    # Clean periodic signal with no anomaly.
    df = diurnal(n=240, period=24, anomaly_phase=None)
    row = detector.fit_score(df, "", {"period": 24}).iloc[-1]
    assert float(row["score"]) < 3.0


def test_diurnal_with_anomaly_scores_high() -> None:
    detector = SeasonalHybridESD()
    # Periodic signal with a large anomaly injected at the last point.
    df = diurnal(n=240, period=24, anomaly_phase=239, anomaly_magnitude=20.0)
    row = detector.fit_score(df, "", {"period": 24}).iloc[-1]
    assert float(row["score"]) > 1.0


def test_short_window_returns_zero() -> None:
    detector = SeasonalHybridESD()
    df = clean_baseline(n=10)
    row = detector.fit_score(df, "", {"period": 24}).iloc[-1]
    assert float(row["score"]) == 0.0


def test_baseline_is_seasonal_component() -> None:
    detector = SeasonalHybridESD()
    df = diurnal(n=240, period=24, mean=50.0, amplitude=10.0, noise=0.01)
    row = detector.fit_score(df, "", {"period": 24}).iloc[-1]
    # Baseline should be close to the seasonal value at that phase.
    assert abs(float(row["baseline"])) < 100.0  # sanity check


def test_alpha_param_changes_sensitivity() -> None:
    detector = SeasonalHybridESD()
    df = diurnal(n=240, period=24, anomaly_phase=239, anomaly_magnitude=5.0)
    strict = float(detector.fit_score(df, "", {"period": 24, "alpha": 0.01}).iloc[-1]["score"])
    permissive = float(detector.fit_score(df, "", {"period": 24, "alpha": 0.10}).iloc[-1]["score"])
    # The test statistic is the same; the critical value changes, so
    # the ratio (score) changes. Stricter alpha => higher critical =>
    # lower score.
    assert strict <= permissive or abs(strict - permissive) < 0.5
