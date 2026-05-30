"""STLResidualMAD detector tests — using a stubbed statsmodels STL."""

from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import pytest

from promanomaly.detectors.stl_residual_mad import STLResidualMAD
from tests.fixtures import clean_baseline


@dataclass
class _StubSTLResult:
    """Minimal STL result stand-in."""

    trend: np.ndarray[Any, Any]
    seasonal: np.ndarray[Any, Any]
    resid: np.ndarray[Any, Any]


class _StubSTL:
    """Minimal statsmodels STL-shaped stand-in.

    Performs a simple median-based seasonal decomposition so the
    detector can be tested end-to-end without the heavy statsmodels
    install.
    """

    def __init__(self, endog: Any, period: int = 24, robust: bool = True) -> None:
        self._values = np.asarray(endog, dtype=float)
        self._period = period
        self._robust = robust

    def fit(self) -> _StubSTLResult:
        n = self._values.size
        seasonal = np.zeros(n)
        for i in range(self._period):
            bucket = self._values[i :: self._period]
            seasonal[i :: self._period] = float(np.median(bucket))
        # Trend = global median (constant), residual = values - seasonal - trend.
        global_median = float(np.median(self._values))
        trend = np.full(n, global_median)
        resid = self._values - seasonal
        return _StubSTLResult(trend=trend, seasonal=seasonal, resid=resid)


@pytest.fixture
def stub_statsmodels(monkeypatch: pytest.MonkeyPatch) -> type[_StubSTL]:
    """Install a fake ``statsmodels.tsa.seasonal`` module backed by :class:`_StubSTL`."""
    seasonal_mod = types.ModuleType("statsmodels.tsa.seasonal")
    seasonal_mod.STL = _StubSTL  # type: ignore[attr-defined]

    tsa_mod = types.ModuleType("statsmodels.tsa")
    tsa_mod.seasonal = seasonal_mod  # type: ignore[attr-defined]

    sm_mod = types.ModuleType("statsmodels")
    sm_mod.tsa = tsa_mod  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "statsmodels", sm_mod)
    monkeypatch.setitem(sys.modules, "statsmodels.tsa", tsa_mod)
    monkeypatch.setitem(sys.modules, "statsmodels.tsa.seasonal", seasonal_mod)
    return _StubSTL


def _frame(values: list[float]) -> pd.DataFrame:
    timestamps = (np.arange(len(values)) * 15.0).astype(float)
    return pd.DataFrame({"timestamp": timestamps, "y": values})


def test_metadata() -> None:
    detector = STLResidualMAD()
    assert detector.name == "STLResidualMAD"
    assert "stl" in detector.description.lower()
    spec_defaults = {p.name: p.default for p in detector.param_spec}
    assert detector.defaults == spec_defaults


def test_clean_baseline_scores_low(stub_statsmodels: type) -> None:
    detector = STLResidualMAD()
    # 60 samples of clean periodic data with period=12.
    n = 60
    vals = [10.0 + 2.0 * np.sin(2 * np.pi * i / 12) for i in range(n)]
    df = _frame(vals)
    row = detector.fit_score(df, "", {"period": 12}).iloc[-1]
    assert float(row["score"]) < 3.0


def test_spike_on_seasonal_scores_high(stub_statsmodels: type) -> None:
    detector = STLResidualMAD()
    n = 60
    vals = [10.0 + 2.0 * np.sin(2 * np.pi * i / 12) for i in range(n)]
    vals[-1] += 20.0  # Inject a large spike.
    df = _frame(vals)
    row = detector.fit_score(df, "", {"period": 12}).iloc[-1]
    assert float(row["score"]) > 3.0


def test_short_window_returns_zero(stub_statsmodels: type) -> None:
    detector = STLResidualMAD()
    df = clean_baseline(n=10)
    row = detector.fit_score(df, "", {"period": 24}).iloc[-1]
    assert float(row["score"]) == 0.0


def test_emits_baseline_bands(stub_statsmodels: type) -> None:
    detector = STLResidualMAD()
    n = 60
    vals = [10.0 + 2.0 * np.sin(2 * np.pi * i / 12) for i in range(n)]
    df = _frame(vals)
    row = detector.fit_score(df, "", {"period": 12}).iloc[-1]
    assert "baseline_lower" in row.index
    assert "baseline_upper" in row.index
    assert float(row["baseline_lower"]) <= float(row["baseline"])
    assert float(row["baseline_upper"]) >= float(row["baseline"])


def test_missing_statsmodels_raises_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "statsmodels", raising=False)
    monkeypatch.delitem(sys.modules, "statsmodels.tsa", raising=False)
    monkeypatch.delitem(sys.modules, "statsmodels.tsa.seasonal", raising=False)

    import builtins

    real_import = builtins.__import__

    def _blocking_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "statsmodels" or name.startswith("statsmodels."):
            raise ImportError("statsmodels stripped for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _blocking_import)
    detector = STLResidualMAD()
    df = _frame([1.0] * 60)
    with pytest.raises(RuntimeError, match="STLResidualMAD requires statsmodels"):
        detector.fit_score(df, "", {"period": 12})
