"""MAD detector tests against the synthetic-series fixtures."""

from __future__ import annotations

from promanomaly.detectors.mad import MAD
from tests.fixtures import (
    clean_baseline,
    clustered_spikes,
    point_spike,
    step_change,
)


def test_mad_metadata() -> None:
    detector = MAD()
    assert detector.name == "MAD"
    assert "rolling" in detector.description.lower()
    assert detector.defaults == {}


def test_clean_baseline_scores_low() -> None:
    detector = MAD()
    df = clean_baseline()
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) < 3.0


def test_point_spike_scores_high() -> None:
    detector = MAD()
    df = point_spike()
    row = detector.fit_score(df, "", {}).iloc[-1]
    # 10-sigma-ish synthetic spike on top of ~0.1 noise — well above 3 MADs.
    assert float(row["score"]) > 3.0


def test_baseline_returned_is_median() -> None:
    detector = MAD()
    df = clean_baseline(mean=42.0, noise=0.01)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert abs(float(row["baseline"]) - 42.0) < 0.05


def test_recent_step_change_scores_high() -> None:
    # When the step landed near the end of the rolling window, only a
    # few samples in the post-step regime have polluted the baseline, so
    # the latest sample (in the new regime) is far from the median in
    # MAD-units. This is the more useful operational case — a step that
    # already happened a long time ago is no longer "anomalous now".
    detector = MAD()
    df = step_change(step=5.0, noise=0.01, change_at=115)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) > 3.0


def test_clustered_spikes_get_through_partially() -> None:
    # Documented limitation: clustered anomalies pollute the baseline.
    # We still expect the latest sample to score >0 (it's anomalous);
    # the fixture is sized so even the polluted baseline yields a
    # meaningful score.
    detector = MAD()
    df = clustered_spikes(cluster=3, spike=10.0, noise=0.01)
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) > 0.0
