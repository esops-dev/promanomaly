"""Tests for the Isolation-Forest detector, using a stubbed sklearn."""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd
import pytest

from promanomaly_iforest import IsolationForest


def _frame(values: list[float]) -> pd.DataFrame:
    timestamps = (np.arange(len(values)) * 15.0).astype(float)
    return pd.DataFrame({"timestamp": timestamps, "y": values})


def test_metadata_advertises_documented_parameters() -> None:
    detector = IsolationForest()
    assert detector.name == "IsolationForest"
    spec_names = {p.name for p in detector.param_spec}
    assert spec_names == {"n_estimators", "contamination"}
    # ``defaults`` and ``param_spec`` defaults must agree.
    spec_defaults = {p.name: p.default for p in detector.param_spec}
    assert detector.defaults == spec_defaults


def test_fit_score_clean_baseline_scores_low(stub_sklearn: type) -> None:
    detector = IsolationForest()
    # 20 samples of clean data around 10.0.
    df = _frame([10.0 + 0.1 * (i % 3 - 1) for i in range(20)])
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) < 3.0


def test_fit_score_spike_scores_high(stub_sklearn: type) -> None:
    detector = IsolationForest()
    # 19 clean samples + one large outlier.
    df = _frame([10.0] * 19 + [50.0])
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) > 3.0


def test_short_window_returns_zero(stub_sklearn: type) -> None:
    detector = IsolationForest()
    df = _frame([1.0, 2.0, 3.0])
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert float(row["score"]) == 0.0


def test_params_forwarded_to_sklearn(monkeypatch: pytest.MonkeyPatch, stub_sklearn: type) -> None:
    captured: dict[str, object] = {}

    class _Capturing(stub_sklearn):  # type: ignore[misc, valid-type]
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            captured.update(kwargs)

    monkeypatch.setattr(sys.modules["sklearn.ensemble"], "IsolationForest", _Capturing)

    IsolationForest().fit_score(
        _frame([1.0] * 20),
        "",
        {"n_estimators": 50, "contamination": 0.10},
    )
    assert captured["n_estimators"] == 50
    assert captured["contamination"] == 0.10


def test_missing_sklearn_raises_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "sklearn", raising=False)
    monkeypatch.delitem(sys.modules, "sklearn.ensemble", raising=False)

    import builtins

    real_import = builtins.__import__

    def _blocking_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "sklearn" or name.startswith("sklearn."):
            raise ImportError("sklearn stripped for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _blocking_import)
    detector = IsolationForest()
    df = _frame([1.0] * 20)
    with pytest.raises(RuntimeError, match="IsolationForest requires scikit-learn"):
        detector.fit_score(df, "", {})


def test_entry_point_is_registered() -> None:
    from importlib.metadata import entry_points

    eps = entry_points(group="promanomaly.detectors")
    names = {ep.name for ep in eps}
    assert "IsolationForest" in names, (
        "IsolationForest not registered — was the package installed via pip "
        "editable install? Try `pip install -e community/promanomaly-iforest`."
    )
