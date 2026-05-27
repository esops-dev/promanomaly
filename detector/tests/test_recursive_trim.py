"""Recursive-trim option on MAD and Hampel.

The clustered-spikes fixture is the worked example from the project
guide: a tight burst of anomalous points inside the rolling window
poisons the median + MAD that robust detectors rely on. With
``recursive_trim`` enabled, the detector should reject the clustered
spikes from the baseline on the trim pass and surface a clearly higher
score on the latest sample than the un-trimmed run.
"""

from __future__ import annotations

from promanomaly.detectors.hampel import Hampel
from promanomaly.detectors.mad import MAD
from tests.fixtures import clean_baseline, clustered_spikes


def test_mad_trim_recovers_baseline_on_clustered_spikes() -> None:
    df = clustered_spikes(cluster=6, spike=10.0, noise=0.05)
    untrimmed = MAD().fit_score(df, "", {}).iloc[-1]
    trimmed = MAD().fit_score(df, "", {"recursive_trim": True}).iloc[-1]
    # The trim should pull the baseline back toward the pre-burst mean
    # (10.0) and inflate the score on the latest sample, which sits well
    # above the clean baseline. The exact numbers are noisy, so we just
    # assert the directional change the operator cares about.
    assert float(trimmed["score"]) > float(untrimmed["score"])
    assert abs(float(trimmed["baseline"]) - 10.0) < abs(float(untrimmed["baseline"]) - 10.0)


def test_mad_trim_is_noop_on_clean_baseline() -> None:
    # No anomalies to trim: the baseline and score should agree with the
    # untrimmed pass to within numerical noise. (Equality isn't quite
    # right because we strip the latest sample on the trim path, which
    # the no-trim path doesn't, so the median can shift by a sample.)
    df = clean_baseline(noise=0.1)
    untrimmed = MAD().fit_score(df, "", {}).iloc[-1]
    trimmed = MAD().fit_score(df, "", {"recursive_trim": True}).iloc[-1]
    assert abs(float(trimmed["score"]) - float(untrimmed["score"])) < 0.5


def test_hampel_trim_recovers_local_baseline_on_clustered_spikes() -> None:
    # With t0=10 the local sub-window is 21 points; a 4-point cluster
    # right at the tail is enough to pull the local median materially
    # without driving MAD to zero. Without trimming Hampel folds the
    # cluster into its baseline and the latest sample looks closer to
    # normal than it should; with trimming the cluster is excluded and
    # the score climbs.
    df = clustered_spikes(cluster=4, spike=10.0, noise=0.05)
    untrimmed = Hampel().fit_score(df, "", {"t0": 10}).iloc[-1]
    trimmed = Hampel().fit_score(df, "", {"t0": 10, "recursive_trim": True}).iloc[-1]
    assert float(trimmed["score"]) > float(untrimmed["score"])


def test_recursive_trim_advertised_in_param_spec() -> None:
    # Discoverability check: ``recursive_trim`` must surface through the
    # detector metadata so ``detectors list`` / ``/debug/detectors``
    # operators can find it without reading source.
    mad_params = {p.name for p in MAD.param_spec}
    hampel_params = {p.name for p in Hampel.param_spec}
    assert {"recursive_trim", "trim_threshold"}.issubset(mad_params)
    assert {"recursive_trim", "trim_threshold"}.issubset(hampel_params)


def test_trim_threshold_param_is_honoured() -> None:
    # Deterministic noisy-baseline fixture: 20 evenly-spaced samples
    # spanning [9.0, 11.0] (clean mean ~10), one mild outlier at 14,
    # one large spike at 100 as the latest. The first-pass baseline
    # sees MAD>0 because the noisy core gives non-degenerate
    # distances; the second-pass MAD differs based on whether the
    # ``14`` is kept (permissive trim) or dropped (aggressive trim).
    # A detector ignoring ``trim_threshold`` would return identical
    # baselines and identical scores for both calls — that's the
    # regression we're guarding against.
    import numpy as np
    import pandas as pd

    noisy = np.linspace(9.0, 11.0, num=20).tolist()
    values = np.array([*noisy, 14.0, 100.0])
    df = pd.DataFrame({"timestamp": np.arange(values.size, dtype=float) * 15.0, "y": values})
    aggressive = MAD().fit_score(df, "", {"recursive_trim": True, "trim_threshold": 1.5}).iloc[-1]
    permissive = MAD().fit_score(df, "", {"recursive_trim": True, "trim_threshold": 10.0}).iloc[-1]
    # Different cutoffs MUST yield different recomputed baselines or
    # MADs — exactly one of (baseline, score) differing is sufficient
    # proof the parameter is wired through.
    assert (float(aggressive["baseline"]) != float(permissive["baseline"])) or (
        float(aggressive["score"]) != float(permissive["score"])
    )
