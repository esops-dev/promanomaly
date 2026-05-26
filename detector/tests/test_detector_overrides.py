"""Per-detector hyperparameter override surface.

The story in v0.2 is documenting (and enforcing) two ways to write a
detector entry:

* string shorthand: ``- MAD``
* dict form with optional params: ``{name: MAD, instance?: ..., params: {...}}``

``params`` is forwarded both as constructor kwargs (when the detector
declares them on ``__init__``) and as the third argument to
``fit_score`` — existing detectors that only consume the latter keep
working, future detectors can stash overrides at construction time.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pandas as pd
import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.detectors import registry
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner, _build_detector
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import clean_baseline


def test_string_shorthand_is_accepted() -> None:
    cfg = Config.model_validate(
        {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://x"},
            "groups": [
                {
                    "name": "g",
                    "queries": [
                        {"id": "m", "promql": "up", "detectors": ["MAD"]},
                    ],
                }
            ],
        }
    )
    entry = cfg.groups[0].queries[0].detectors[0]
    assert entry.name == "MAD"
    assert entry.instance is None
    assert entry.params == {}


def test_mixed_shorthand_and_dict_form_is_accepted() -> None:
    cfg = Config.model_validate(
        {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://x"},
            "groups": [
                {
                    "name": "g",
                    "queries": [
                        {
                            "id": "m",
                            "promql": "up",
                            "detectors": [
                                "MAD",
                                {"name": "Hampel", "params": {"t0": 5}},
                            ],
                        },
                    ],
                }
            ],
        }
    )
    detectors = cfg.groups[0].queries[0].detectors
    assert detectors[0].name == "MAD"
    assert detectors[1].name == "Hampel"
    assert detectors[1].params == {"t0": 5}


def test_string_shorthand_rejects_bad_name() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Config.model_validate(
            {
                "apiVersion": CURRENT_API_VERSION,
                "datasource": {"url": "http://x"},
                "groups": [
                    {
                        "name": "g",
                        "queries": [{"id": "m", "promql": "up", "detectors": ["with:colon"]}],
                    }
                ],
            }
        )


def test_build_detector_passes_matching_kwargs() -> None:
    class Configurable:
        name: ClassVar[str] = "Cfg"
        description: ClassVar[str] = "captures constructor kwargs"
        defaults: ClassVar[dict[str, Any]] = {}

        def __init__(self, alpha: float = 0.1, beta: int = 1) -> None:
            self.alpha = alpha
            self.beta = beta

        def fit_score(
            self, df: pd.DataFrame, freq: str, params: dict[str, Any]
        ) -> pd.DataFrame:  # pragma: no cover - unused in this unit test
            return pd.DataFrame()

    # Matching kwargs are forwarded; unknown ones are dropped silently so
    # detectors stay forward-compatible when the runner adds new defaults.
    instance = _build_detector(Configurable, {"alpha": 0.25, "extra": "ignored"})
    assert isinstance(instance, Configurable)
    assert instance.alpha == 0.25
    assert instance.beta == 1


def test_build_detector_with_no_init_kwargs_uses_default_ctor() -> None:
    class NoArgs:
        name: ClassVar[str] = "Noargs"
        description: ClassVar[str] = ""
        defaults: ClassVar[dict[str, Any]] = {}

        def fit_score(
            self, df: pd.DataFrame, freq: str, params: dict[str, Any]
        ) -> pd.DataFrame:  # pragma: no cover
            return pd.DataFrame()

    instance = _build_detector(NoArgs, {"anything": 1})
    assert isinstance(instance, NoArgs)


def test_build_detector_with_var_keyword_receives_full_params() -> None:
    captured: dict[str, Any] = {}

    class StarStar:
        name: ClassVar[str] = "Starstar"
        description: ClassVar[str] = ""
        defaults: ClassVar[dict[str, Any]] = {}

        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

        def fit_score(
            self, df: pd.DataFrame, freq: str, params: dict[str, Any]
        ) -> pd.DataFrame:  # pragma: no cover
            return pd.DataFrame()

    _build_detector(StarStar, {"a": 1, "b": 2})
    assert captured == {"a": 1, "b": 2}


@pytest.mark.asyncio
async def test_calibration_uses_constructor_kwargs(stub_source: StubSource) -> None:
    """Regression: calibration must instantiate detectors the same way
    the live detection path does. Without this, a detector that consumes
    its tuning knobs in ``__init__`` would silently never receive a
    confidence / baseline-stability gauge — calibration would raise
    ``TypeError`` and the runner would swallow the exception.
    """
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "h"}, df)]))

    class RequiresCtorKwarg:
        name: ClassVar[str] = "Required"
        description: ClassVar[str] = ""
        defaults: ClassVar[dict[str, Any]] = {}

        # No default → calls without ``k=`` would TypeError. The runner
        # MUST forward params to the constructor in both detection and
        # calibration paths.
        def __init__(self, k: float) -> None:
            self.k = k

        def fit_score(self, df: pd.DataFrame, freq: str, params: dict[str, Any]) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "timestamp": float(df["timestamp"].iloc[-1]),
                        "score": self.k,
                        "baseline": 0.0,
                        "is_outside": False,
                    }
                ]
            )

    registry.register(RequiresCtorKwarg)
    try:
        cfg = Config.model_validate(
            {
                "apiVersion": CURRENT_API_VERSION,
                "datasource": {"url": "http://stub"},
                "defaults": {
                    "window": "15m",
                    "step": "15s",
                    "min_points": 30,
                    "warmup_policy": "emit_warming_up",
                    "alert_thresholds": {"score": 3.0},
                    # Calibration runs because emit_baseline_stability is
                    # the default ``true``. The test would still cover
                    # detection-path instantiation even without it.
                    "emit_baseline_stability": True,
                },
                "groups": [
                    {
                        "name": "g",
                        "queries": [
                            {
                                "id": "m",
                                "promql": "up",
                                "detectors": [
                                    {"name": "Required", "params": {"k": 2.5}},
                                ],
                            }
                        ],
                    }
                ],
            }
        )
        store = SnapshotStore()
        ops = OperationalMetrics()
        runner = Runner(cfg, stub_source, store, ops)
        result = await runner.run_group("g")
        assert result.succeeded
        # Calibration must have run successfully — the confidence gauge
        # only appears when ``calibrate_detector`` completed without
        # raising. If the construction bug were present this assertion
        # would fail.
        metrics = {s.metric for s in result.samples}
        assert "anomaly_confidence_score" in metrics
        assert "anomaly_baseline_stability" in metrics
    finally:
        registry.unregister("Required")


@pytest.mark.asyncio
async def test_runner_forwards_constructor_kwargs(stub_source: StubSource) -> None:
    """End-to-end: a detector that consumes its tuning knob in __init__
    sees the values the operator put in the YAML."""
    df = clean_baseline(n=120)
    stub_source.respond(lambda _: StubResponse(series=[make_series({"instance": "h"}, df)]))

    captured: list[dict[str, Any]] = []

    class CtorDetector:
        name: ClassVar[str] = "Ctor"
        description: ClassVar[str] = ""
        defaults: ClassVar[dict[str, Any]] = {}

        def __init__(self, k: float = 1.5) -> None:
            self.k = k
            captured.append({"k": k})

        def fit_score(self, df: pd.DataFrame, freq: str, params: dict[str, Any]) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "timestamp": float(df["timestamp"].iloc[-1]),
                        "score": self.k,
                        "baseline": 0.0,
                        "is_outside": False,
                    }
                ]
            )

    registry.register(CtorDetector)
    try:
        cfg = Config.model_validate(
            {
                "apiVersion": CURRENT_API_VERSION,
                "datasource": {"url": "http://stub"},
                "defaults": {
                    "window": "15m",
                    "step": "15s",
                    "min_points": 30,
                    "warmup_policy": "emit_warming_up",
                    "alert_thresholds": {"score": 3.0},
                },
                "groups": [
                    {
                        "name": "g",
                        "queries": [
                            {
                                "id": "m",
                                "promql": "up",
                                "detectors": [
                                    {"name": "Ctor", "params": {"k": 3.0}},
                                ],
                            }
                        ],
                    }
                ],
            }
        )
        store = SnapshotStore()
        ops = OperationalMetrics()
        runner = Runner(cfg, stub_source, store, ops)
        result = await runner.run_group("g")
        assert result.succeeded
        # Capture list has at least one entry with the configured k=3.0.
        assert any(c["k"] == 3.0 for c in captured)
        # The emitted score reflects the configured k value, proving the
        # constructor saw the params.
        score = next(s for s in result.samples if s.metric == "anomaly_score")
        assert score.value == pytest.approx(3.0)
    finally:
        registry.unregister("Ctor")
