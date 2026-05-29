"""Operator-facing severity signal.

Raw ``anomaly_score`` units differ across detectors — MADs for
MAD/Hampel/IQR/HourOfDayMAD/DayOfWeekMAD/Cohort, a posterior probability
for BOCPD, a sigma multiplier for CUSUM. None of them answer the
on-call question "how much should I care about *this* firing relative
to the others lighting up right now?".

:func:`compute_severity` folds the disparate inputs into one normalised
``0-1`` gauge so several simultaneous firings can be ranked. It is a
**documented, deterministic** combination with **fixed weights** — the
exact formula lives in ``docs/severity.md`` and is deliberately not
user-tunable. Operators who want a different weighting compose via
recording rules; baking tunable weights into the detector drifts toward
the configuration sprawl the project guide warns against.

Severity is signal enrichment, **not** an alerting decision: alerting
still happens via PrometheusRules on the metric (the ``AnomalyHighSeverity``
reference alert). It is distinct from ``anomaly_confidence_score`` (a
single detector's quality) and ``anomaly_composite_score`` (cross-detector
fusion).
"""

from __future__ import annotations

# Detector families whose ``anomaly_score`` is already a probability in
# ``[0, 1]`` rather than a threshold-relative distance. Their strength is
# the score itself; everyone else is squashed relative to the alert
# threshold. Kept as a frozenset so membership checks stay O(1) and the
# set is obvious to a reader scanning for "which detectors are special".
PROBABILITY_DETECTORS: frozenset[str] = frozenset({"BOCPD"})

# Duration ramp saturation point, in seconds. Matches the
# ``AnomalyLongRunning`` reference alert (30 minutes): a firing that has
# been continuous for half an hour contributes its full duration weight.
_DURATION_SATURATION_SECONDS: float = 1800.0

# Fixed severity weights. They sum to 1.0 so the trust/duration
# multiplier stays in ``[W_STRENGTH, 1.0]`` and severity never exceeds
# ``strength``. Changing these shifts every operator's ranking and is a
# breaking change to the documented formula — bump the doc and treat it
# like a metric rename.
_W_STRENGTH: float = 0.40
_W_CONFIDENCE: float = 0.30
_W_STABILITY: float = 0.15
_W_DURATION: float = 0.15


def _clamp01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def squash_strength(score: float, threshold: float, detector_name: str) -> float:
    """Map a detector's raw score onto a comparable ``0-1`` strength.

    Probability detectors (BOCPD) pass their score through directly —
    it is already a calibrated ``[0, 1]`` quantity. Every other detector
    is scored relative to its own alert threshold: the strength reaches
    ``1.0`` at twice the threshold (e.g. 6 MADs when the threshold is 3),
    which keeps a 3-MAD firing and a 0.6-probability firing on the same
    "moderately severe" footing rather than letting unbounded MAD
    distances dwarf the probability detectors.
    """
    if detector_name in PROBABILITY_DETECTORS:
        return _clamp01(score)
    if threshold <= 0.0:
        # Degenerate threshold: any positive score is "fully severe".
        return 1.0 if score > 0.0 else 0.0
    return _clamp01(score / (2.0 * threshold))


def duration_ramp(duration_seconds: float) -> float:
    """Linear ramp from 0 to 1 as a firing persists, saturating at 30m."""
    if duration_seconds <= 0.0:
        return 0.0
    return _clamp01(duration_seconds / _DURATION_SATURATION_SECONDS)


def compute_severity(
    *,
    score: float,
    threshold: float,
    detector_name: str,
    outside: bool,
    confidence: float | None,
    baseline_stability: float | None,
    duration_seconds: float,
) -> float:
    """Combine the inputs into a normalised ``0-1`` severity.

    ``outside`` is the post-practical-significance verdict from the
    runner — when ``False`` (the firing was gated out, statistically or
    by a practical-significance floor) severity is ``0.0`` regardless of
    the raw score, so a sub-threshold blip never ranks above a real
    firing. When the series is firing, severity is the squashed
    statistical strength scaled by a trust/duration multiplier::

        severity = strength * (W_STRENGTH
                               + W_CONFIDENCE * confidence
                               + W_STABILITY  * stability
                               + W_DURATION   * duration_ramp)

    ``confidence`` / ``baseline_stability`` come from the calibration
    cycle and may be ``None`` before calibration has run; they default
    to ``1.0`` (no penalty) so an un-calibrated but strongly-firing
    series still ranks high rather than being suppressed for lack of a
    secondary signal. The multiplier stays in ``[W_STRENGTH, 1.0]``, so
    severity never exceeds ``strength``.
    """
    if not outside:
        return 0.0
    strength = squash_strength(score, threshold, detector_name)
    conf = 1.0 if confidence is None else _clamp01(confidence)
    stab = 1.0 if baseline_stability is None else _clamp01(baseline_stability)
    ramp = duration_ramp(duration_seconds)
    multiplier = _W_STRENGTH + _W_CONFIDENCE * conf + _W_STABILITY * stab + _W_DURATION * ramp
    return _clamp01(strength * multiplier)


__all__ = [
    "PROBABILITY_DETECTORS",
    "compute_severity",
    "duration_ramp",
    "squash_strength",
]
