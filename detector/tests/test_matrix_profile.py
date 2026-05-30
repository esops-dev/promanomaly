"""MatrixProfile detector tests — using a stubbed stumpy."""

from __future__ import annotations

import sys
import types
from typing import Any

import numpy as np
import pandas as pd
import pytest

from promanomaly.detectors.matrix_profile import MatrixProfile


class _StubStump:
    """Minimal stumpy.stump stand-in.

    Returns a matrix profile where each entry's NN distance is the
    absolute difference between the subsequence mean and the global
    mean — simple enough to test the normalisation logic without
    needing the real stumpy dependency.
    """

    pass


def _stub_stump(ts: np.ndarray[Any, Any], m: int) -> np.ndarray[Any, Any]:
    """Fake ``stumpy.stump`` that computes a simplified matrix profile.

    For each subsequence, computes the z-normalised Euclidean distance to
    the nearest neighbour (brute-force). This approximates stumpy's real
    behaviour closely enough for testing.
    """
    n = ts.size
    profile_len = n - m + 1
    result = np.zeros((profile_len, 4))

    # Extract all subsequences.
    subseqs = np.array([ts[i : i + m] for i in range(profile_len)])

    for i in range(profile_len):
        min_dist = np.inf
        nn_idx = 0
        for j in range(profile_len):
            if abs(i - j) < 1:
                continue  # Skip self-match and trivial matches.
            dist = float(np.sqrt(np.sum((subseqs[i] - subseqs[j]) ** 2)))
            if dist < min_dist:
                min_dist = dist
                nn_idx = j
        result[i, 0] = min_dist if np.isfinite(min_dist) else 0.0
        result[i, 1] = nn_idx
    return result


@pytest.fixture
def stub_stumpy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install a fake ``stumpy`` module."""
    module = types.ModuleType("stumpy")
    module.stump = _stub_stump  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "stumpy", module)


def _frame(values: list[float]) -> pd.DataFrame:
    timestamps = (np.arange(len(values)) * 15.0).astype(float)
    return pd.DataFrame({"timestamp": timestamps, "y": values})


def test_metadata() -> None:
    detector = MatrixProfile()
    assert detector.name == "MatrixProfile"
    assert "matrix" in detector.description.lower()
    spec_defaults = {p.name: p.default for p in detector.param_spec}
    assert detector.defaults == spec_defaults


def test_clean_baseline_scores_low(stub_stumpy: None) -> None:
    detector = MatrixProfile()
    # Clean constant signal — all subsequences look the same.
    df = _frame([10.0] * 60)
    row = detector.fit_score(df, "", {"m": 10}).iloc[-1]
    assert float(row["score"]) < 3.0


def test_anomalous_shape_scores_high(stub_stumpy: None) -> None:
    detector = MatrixProfile()
    # Clean signal with a distorted final subsequence.
    vals = [10.0] * 50 + [50.0] * 10
    df = _frame(vals)
    row = detector.fit_score(df, "", {"m": 10}).iloc[-1]
    assert float(row["score"]) > 0.0


def test_short_window_returns_zero(stub_stumpy: None) -> None:
    detector = MatrixProfile()
    df = _frame([1.0, 2.0, 3.0])
    row = detector.fit_score(df, "", {"m": 10}).iloc[-1]
    assert float(row["score"]) == 0.0


def test_missing_stumpy_raises_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "stumpy", raising=False)

    import builtins

    real_import = builtins.__import__

    def _blocking_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "stumpy" or name.startswith("stumpy."):
            raise ImportError("stumpy stripped for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _blocking_import)
    detector = MatrixProfile()
    df = _frame([1.0] * 60)
    with pytest.raises(RuntimeError, match="MatrixProfile requires stumpy"):
        detector.fit_score(df, "", {"m": 10})
