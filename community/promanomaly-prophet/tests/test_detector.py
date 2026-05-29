"""Tests for the Prophet-residual detector, using a stubbed Prophet."""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd
import pytest

from promanomaly_prophet import ProphetResidual


def _frame(values: list[float]) -> pd.DataFrame:
    timestamps = (np.arange(len(values)) * 15.0).astype(float)
    return pd.DataFrame({"timestamp": timestamps, "y": values})


def test_metadata_advertises_documented_parameters() -> None:
    detector = ProphetResidual()
    assert detector.name == "ProphetResidual"
    spec_names = {p.name for p in detector.param_spec}
    assert spec_names == {"changepoint_prior_scale", "interval_width", "seasonality_mode"}
    # ``defaults`` and ``param_spec`` defaults must agree — same contract
    # the host detector's registry test enforces.
    spec_defaults = {p.name: p.default for p in detector.param_spec}
    assert detector.defaults == spec_defaults


def test_fit_score_normalises_residual_by_band(stub_prophet: type) -> None:
    detector = ProphetResidual()
    # 11 samples ending with the spike. Stub forecast: yhat = y[-2]
    # (=10.0) and band half-width = 1.0 -> score == |20 - 10| / 1 = 10.
    df = _frame([10.0] * 10 + [20.0])
    row = detector.fit_score(df, "", {}).iloc[-1]
    assert row["baseline"] == pytest.approx(10.0)
    assert row["baseline_lower"] == pytest.approx(9.0)
    assert row["baseline_upper"] == pytest.approx(11.0)
    assert row["score"] == pytest.approx(10.0)
    # Use ``bool(...)`` to coerce numpy.bool_ to a plain bool for the
    # identity check — pandas returns np.False_ which isn't ``is False``.
    assert bool(row["is_outside"]) is False


def test_params_forwarded_to_prophet(monkeypatch: pytest.MonkeyPatch, stub_prophet: type) -> None:
    # Sanity check: custom params reach the Prophet constructor. We
    # wrap the stub with a capturing subclass that records the kwargs
    # it was called with — simpler than threading state through the
    # base stub.
    captured: dict[str, object] = {}

    class _Capturing(stub_prophet):  # type: ignore[misc, valid-type]
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            captured.update(kwargs)

    monkeypatch.setattr(sys.modules["prophet"], "Prophet", _Capturing)

    ProphetResidual().fit_score(
        _frame([1.0, 2.0, 3.0, 4.0]),
        "",
        {
            "changepoint_prior_scale": 0.20,
            "interval_width": 0.50,
            "seasonality_mode": "multiplicative",
        },
    )
    assert captured == {
        "changepoint_prior_scale": 0.20,
        "interval_width": 0.50,
        "seasonality_mode": "multiplicative",
    }


def test_missing_prophet_raises_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # Drop both the stub and any cached real prophet from sys.modules,
    # and block re-imports so the lazy import inside fit_score really
    # has to fail with our wrapped error.
    monkeypatch.delitem(sys.modules, "prophet", raising=False)

    import builtins

    real_import = builtins.__import__

    def _blocking_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "prophet" or name.startswith("prophet."):
            raise ImportError("prophet stripped for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _blocking_import)
    detector = ProphetResidual()
    df = _frame([1.0, 2.0, 3.0, 4.0])
    with pytest.raises(RuntimeError, match="ProphetResidual requires the 'prophet' package"):
        detector.fit_score(df, "", {})


def test_entry_point_is_registered() -> None:
    # The package declares the entry point in pyproject.toml; verify
    # importlib.metadata picks it up when the package is installed.
    from importlib.metadata import entry_points

    eps = entry_points(group="promanomaly.detectors")
    names = {ep.name for ep in eps}
    assert "ProphetResidual" in names, (
        "ProphetResidual not registered — was the package installed via pip "
        "editable install? Try `pip install -e community/promanomaly-prophet`."
    )
