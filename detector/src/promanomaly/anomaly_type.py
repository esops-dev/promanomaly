"""Anomaly typing — classify *what kind* of anomaly is firing.

When a series fires, on-call wants to know whether it is a point spike,
a step up/down, a slow level drift, a variance/distribution shift, or a
cohort divergence — so they can route, runbook, and filter without first
opening the signal. The detectors already compute most of this internally
(CUSUM direction, the cohort delta, the deviation shape inside the
rolling window); this module surfaces it as a frozen, public value-set.

The ``type`` value-set is **public API**, enumerated in
``docs/detectors.md`` and treated like a metric name: adding a value is
non-breaking, renaming or removing one is breaking. The classifier is
deterministic — same window in, same type out — so the
``anomaly_type_score`` gauge and the triage endpoint never disagree.

The classification is heuristic by design: it reads the rolling window
the detector already scored against, so it adds no extra TSDB load and
no per-detector protocol changes. Detectors that want to override the
heuristic can populate an ``anomaly_type`` column in their score row and
it is used verbatim (after validation against the frozen set).
"""

from __future__ import annotations

from typing import Any

import numpy as np

# Frozen, public value-set. See module docstring for the stability
# contract. ``shape_discord`` is reserved for the future shape/discord
# detectors and is listed here so the enum is stable when they land;
# nothing emits it today.
ANOMALY_TYPES: frozenset[str] = frozenset(
    {
        "point_spike",
        "step_up",
        "step_down",
        "level_drift",
        "variance_shift",
        "cohort_divergence",
        "shape_discord",
    }
)

# Tuning constants for the window heuristic. Expressed in robust-sigma
# (MAD-scaled) units so they are detector-agnostic.
_STEP_LEVEL_SIGMA: float = 2.0  # half-to-half median shift that reads as a step
# Fraction of the half-to-half delta that one consecutive jump must
# carry for the shift to read as an abrupt step rather than a drift.
_STEP_JUMP_FRACTION: float = 0.5
_VARIANCE_RATIO: float = 2.5  # tail-spread / head-spread that reads as variance shift
_DRIFT_MONOTONIC_FRACTION: float = 0.7  # |rank-correlation| above this reads as drift
_MAD_TO_SIGMA: float = 1.4826  # MAD -> Gaussian sigma scale factor


def _robust_scale(values: np.ndarray[Any, Any]) -> float:
    """MAD-derived sigma, falling back to std then to 1.0 on a flatline."""
    median = float(np.median(values))
    mad = float(np.median(np.abs(values - median)))
    if mad > 0.0:
        return mad * _MAD_TO_SIGMA
    std = float(np.std(values))
    return std if std > 0.0 else 1.0


def _monotonic_fraction(values: np.ndarray[Any, Any]) -> float:
    """|Spearman-style rank correlation with the index|, in ``[0, 1]``.

    A clean monotonic drift correlates near ±1 with the time index; a
    one-off step or spike does not. Computed on ranks so a single outlier
    can't dominate the slope the way a raw-value regression would.
    """
    n = values.size
    if n < 3:
        return 0.0
    index_ranks = np.arange(n, dtype=float)
    order = np.argsort(values, kind="stable")
    value_ranks = np.empty(n, dtype=float)
    value_ranks[order] = np.arange(n, dtype=float)
    # Pearson correlation of the two rank vectors == Spearman's rho.
    idx_c = index_ranks - index_ranks.mean()
    val_c = value_ranks - value_ranks.mean()
    denom = float(np.sqrt(np.sum(idx_c**2) * np.sum(val_c**2)))
    if denom == 0.0:
        return 0.0
    return abs(float(np.sum(idx_c * val_c) / denom))


def classify_window(
    *,
    detector_name: str,
    values: np.ndarray[Any, Any],
    baseline: float,
    latest_y: float,
    change_point_fired: bool,
    cohort_aware: bool,
) -> str:
    """Return the anomaly type for a firing series from its rolling window.

    Priority order:

    1. Cohort detectors → ``cohort_divergence`` (the anomaly is
       cross-member, not intra-series).
    2. A change-point detector that fired → directional step
       (``step_up`` / ``step_down``) by the sign of the latest value vs.
       its baseline.
    3. Otherwise read the window shape: a sustained half-to-half level
       shift reads as a step (or, if monotonic across the whole window,
       ``level_drift``); an elevated recent spread with a stable level
       reads as ``variance_shift``; a lone deviating latest sample reads
       as ``point_spike`` (the default).
    """
    if cohort_aware:
        return "cohort_divergence"
    if change_point_fired:
        return "step_up" if latest_y >= baseline else "step_down"

    n = int(values.size)
    if n < 4:
        return "point_spike"

    mid = n // 2
    head = values[:mid]
    tail = values[mid:]
    head_med = float(np.median(head))
    tail_med = float(np.median(tail))
    raw_level_delta = abs(tail_med - head_med)

    head_spread = float(np.median(np.abs(head - head_med)))
    tail_spread = float(np.median(np.abs(tail - tail_med)))

    # Scale by the *within-half* noise, not the whole-window spread: a
    # step (or a strong drift) inflates the global MAD enough to mask
    # itself, so a global scale would shrink level_delta below the
    # threshold and the shift would never register.
    noise_mad = max(head_spread, tail_spread)
    scale = noise_mad * _MAD_TO_SIGMA if noise_mad > 0.0 else _robust_scale(values)
    level_delta = raw_level_delta / scale

    if level_delta >= _STEP_LEVEL_SIGMA:
        # A sustained shift between halves. Distinguish an abrupt step
        # from a gradual drift by where the movement happens: a step
        # concentrates it in one consecutive jump, while a drift spreads
        # it evenly across the window. The largest single sample-to-sample
        # jump as a fraction of the half-to-half delta separates them
        # cleanly — a rank-correlation test can't, because a clean step
        # is also perfectly monotonic in rank.
        diffs = np.abs(np.diff(values))
        max_jump = float(diffs.max()) if diffs.size else 0.0
        if max_jump >= _STEP_JUMP_FRACTION * raw_level_delta:
            return "step_up" if tail_med >= head_med else "step_down"
        return "level_drift"

    # Variance shift: comparable level, materially wider recent spread.
    if head_spread > 0.0 and (tail_spread / head_spread) >= _VARIANCE_RATIO:
        return "variance_shift"

    # A monotonic crawl that never reached a full half-to-half step is
    # still a drift if the trend is strong.
    if _monotonic_fraction(values) >= _DRIFT_MONOTONIC_FRACTION:
        return "level_drift"

    return "point_spike"


def coerce_declared_type(value: Any) -> str | None:
    """Validate a detector-declared ``anomaly_type`` against the frozen set.

    Returns the value when it is a recognised type, else ``None`` so the
    caller falls back to the window heuristic. Keeps a typo in a
    third-party detector from leaking an unbounded ``type`` label value
    onto the monitoring plane.
    """
    if isinstance(value, str) and value in ANOMALY_TYPES:
        return value
    return None


__all__ = [
    "ANOMALY_TYPES",
    "classify_window",
    "coerce_declared_type",
]
