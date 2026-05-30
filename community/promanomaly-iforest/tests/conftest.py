"""Test fixtures: an sklearn IsolationForest stub so tests run without the heavy real install.

scikit-learn is a large dependency; the detector's job is to call
``IsolationForest(...).fit(...).score_samples(...)`` and read the raw
scores, so we stub a minimal IsolationForest-shaped object that mirrors
those calls. Real sklearn behaviour is validated by sklearn's own tests;
ours pin the contract between the detector and the library.
"""

from __future__ import annotations

import sys
import types
from typing import Any

import numpy as np
import pytest


class _StubIsolationForest:
    """Minimal IsolationForest-shaped stand-in.

    Returns anomaly scores based on distance from the median — outliers
    get more negative scores, matching sklearn's convention.
    """

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self._median: float = 0.0
        self._mad: float = 1.0

    def fit(self, x: np.ndarray[Any, Any]) -> _StubIsolationForest:
        values = x.ravel()
        self._median = float(np.median(values))
        mad = float(np.median(np.abs(values - self._median)))
        self._mad = mad if mad > 0.0 else 1.0
        return self

    def score_samples(self, x: np.ndarray[Any, Any]) -> np.ndarray[Any, Any]:
        # More negative = more anomalous, mirroring sklearn's convention.
        values = x.ravel()
        distances = np.abs(values - self._median) / self._mad
        # Baseline score near 0 for normal, negative for anomalous.
        return -distances


@pytest.fixture
def stub_sklearn(monkeypatch: pytest.MonkeyPatch) -> type[_StubIsolationForest]:
    """Install a fake ``sklearn`` module backed by :class:`_StubIsolationForest`.

    Replaces any real sklearn install for the duration of the test;
    teardown restores the original entry from sys.modules.
    """
    ensemble_module = types.ModuleType("sklearn.ensemble")
    ensemble_module.IsolationForest = _StubIsolationForest  # type: ignore[attr-defined]

    sklearn_module = types.ModuleType("sklearn")
    sklearn_module.ensemble = ensemble_module  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "sklearn", sklearn_module)
    monkeypatch.setitem(sys.modules, "sklearn.ensemble", ensemble_module)
    return _StubIsolationForest
