"""Test fixtures: a Prophet stub so tests run without the heavy real install.

Prophet pulls in cmdstan / pystan, which is a multi-minute install on
fresh CI runners and brittle to compiler-toolchain drift. The
detector's job is to call ``Prophet(...).fit(...).predict(...)`` and
read three columns out of the forecast frame, so we stub a minimal
Prophet-shaped object that mirrors those three calls. Real Prophet
behaviour is validated by Prophet's own tests; ours pin the
contract between the detector and the library.
"""

from __future__ import annotations

import sys
import types
from typing import Any

import pandas as pd
import pytest


class _StubProphet:
    """Minimal Prophet-shaped stand-in.

    Stores constructor kwargs (so tests can assert they were
    forwarded), records the call history, and returns a forecast frame
    that deterministically depends on the input ``y`` so the detector
    can be tested end-to-end.
    """

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.history: pd.DataFrame | None = None

    def fit(self, df: pd.DataFrame) -> _StubProphet:
        self.history = df.copy()
        return self

    def predict(self, future: pd.DataFrame) -> pd.DataFrame:
        # yhat = last-but-one historical y (a stand-in baseline);
        # band half-width = 1.0 so score == |y - yhat|.
        assert self.history is not None, "predict called before fit"
        baseline = float(self.history["y"].iloc[-2]) if len(self.history) > 1 else 0.0
        n = len(future)
        return pd.DataFrame(
            {
                "ds": future["ds"].to_numpy(),
                "yhat": [baseline] * n,
                "yhat_lower": [baseline - 1.0] * n,
                "yhat_upper": [baseline + 1.0] * n,
            }
        )


@pytest.fixture
def stub_prophet(monkeypatch: pytest.MonkeyPatch) -> type[_StubProphet]:
    """Install a fake ``prophet`` module backed by :class:`_StubProphet`.

    Replaces any real ``prophet`` install for the duration of the test;
    teardown restores the original entry from sys.modules.
    """
    module = types.ModuleType("prophet")
    module.Prophet = _StubProphet  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "prophet", module)
    return _StubProphet
