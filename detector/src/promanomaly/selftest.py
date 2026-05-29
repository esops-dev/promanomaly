"""End-to-end detection self-test (dead-man's-switch).

Drives one synthetic series carrying a known injected point-spike anomaly
through the *real* detect → threshold → export path each run and asserts
it is caught. The point is to prove the whole pipeline is live, not just
that the process is up: a global misconfiguration — an exporter
regression, an absurd ``alert_thresholds.score``, a detector that silently
returns nothing — can leave promanomaly running and emitting no anomalies
while the per-detector success ratios still look healthy.

Built on the same synthetic-injection primitive the calibration cycle
uses (:data:`promanomaly.calibration.INJECTION_PATTERNS`), stateless, and
bounded to one synthetic series. The result drives ``anomaly_selftest_ok``
(1/0) and ``anomaly_selftest_failures_total``; the ``AnomalyPipelineDead``
reference alert fires when it stops passing.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import structlog

from .calibration import INJECTION_PATTERNS
from .detectors import UnknownDetectorError
from .detectors.registry import get as get_detector
from .exporter import InvalidMetricNameError, render_samples
from .plan import build_detector
from .state import Sample

logger = structlog.get_logger(__name__)

# Deterministic synthetic window: a clean baseline with a small amount of
# fixed-seed noise (so the baseline MAD/std is non-zero and the detector
# isn't dividing by zero) plus a single injected point spike on the last
# sample. The seed is fixed so the self-test is reproducible run-to-run —
# a flapping self-test would be worse than none.
_SEED = 1729
_BASELINE = 100.0
_NOISE_SD = 1.0
# Floor on the synthetic window length so even a tiny ``min_points`` still
# gives the detector enough context to compute a stable baseline.
_MIN_WINDOW = 60


@dataclass(frozen=True)
class SelfTestResult:
    """Outcome of one self-test run."""

    ok: bool
    detector: str
    score: float
    threshold: float
    outside: bool
    detail: str

    def to_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "detector": self.detector,
            "score": self.score,
            "threshold": self.threshold,
            "outside": self.outside,
            "detail": self.detail,
        }


def _synthetic_window(min_points: int) -> pd.DataFrame:
    n = max(min_points, _MIN_WINDOW)
    rng = np.random.default_rng(_SEED)
    clean = _BASELINE + rng.normal(0.0, _NOISE_SD, size=n)
    spiked = INJECTION_PATTERNS["point_spike"](clean)
    # 15s spacing mirrors the default scoring step; the exact cadence is
    # irrelevant to a point-spike detector but keeps the frame realistic.
    timestamps = np.arange(n, dtype=float) * 15.0
    return pd.DataFrame({"timestamp": timestamps, "y": spiked})


def run_selftest(*, detector_name: str, threshold: float, min_points: int) -> SelfTestResult:
    """Run one synthetic detect → threshold → export cycle.

    Returns ``ok=True`` only when the injected anomaly is both flagged
    outside threshold by the detector *and* survives rendering through the
    exporter (so an exporter regression that rejects the sample also trips
    the self-test). Any exception along the way is caught and reported as
    a failure rather than raised — the dead-man's-switch must itself never
    take the process down.
    """
    try:
        cls = get_detector(detector_name)
    except UnknownDetectorError as exc:
        return SelfTestResult(
            ok=False,
            detector=detector_name,
            score=0.0,
            threshold=threshold,
            outside=False,
            detail=f"unknown detector: {exc}",
        )

    params = dict(getattr(cls, "defaults", {}) or {})
    window = _synthetic_window(min_points)
    try:
        detector = build_detector(cls, params)
        row = detector.fit_score(window, "", params).iloc[-1]
    except Exception as exc:
        return SelfTestResult(
            ok=False,
            detector=detector_name,
            score=0.0,
            threshold=threshold,
            outside=False,
            detail=f"detector raised: {exc}",
        )

    score = float(row.get("score", 0.0))
    # Mirror the runner's verdict: a detector that owns its threshold
    # semantics flags ``is_outside`` directly; otherwise compare the raw
    # score. No practical-significance floors apply — the synthetic spike
    # is a pure statistical signal.
    outside = bool(row.get("is_outside", False)) or score >= threshold

    # Exercise the export path with the same Sample shape the runner emits,
    # so an exporter regression (e.g. a name that fails validation) is
    # caught here rather than silently blanking /metrics.
    labels: tuple[tuple[str, str], ...] = (
        ("detector", detector_name),
        ("group", "_selftest"),
        ("id", "_selftest"),
    )
    samples = [
        Sample(metric="anomaly_score", labels=labels, value=score),
        Sample(
            metric="anomaly_outside_threshold",
            labels=labels,
            value=1.0 if outside else 0.0,
        ),
    ]
    try:
        rendered = render_samples(samples)
    except InvalidMetricNameError as exc:
        return SelfTestResult(
            ok=False,
            detector=detector_name,
            score=score,
            threshold=threshold,
            outside=outside,
            detail=f"export path rejected the sample: {exc}",
        )

    exported_fired = "anomaly_outside_threshold{" in rendered and "} 1.0" in rendered
    if not outside:
        return SelfTestResult(
            ok=False,
            detector=detector_name,
            score=score,
            threshold=threshold,
            outside=False,
            detail=(
                "injected anomaly not caught: score "
                f"{score:.3f} did not cross threshold {threshold}"
            ),
        )
    if not exported_fired:
        return SelfTestResult(
            ok=False,
            detector=detector_name,
            score=score,
            threshold=threshold,
            outside=outside,
            detail="detector fired but the export path did not surface it",
        )
    return SelfTestResult(
        ok=True,
        detector=detector_name,
        score=score,
        threshold=threshold,
        outside=True,
        detail="ok",
    )


__all__ = ["SelfTestResult", "run_selftest"]
