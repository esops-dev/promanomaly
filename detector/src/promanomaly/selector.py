"""Per-series auto-selection of the best-performing detector.

When ``auto_select`` is enabled on a group, the runner periodically
calibrates every configured detector against an injected synthetic
anomaly and stores a :class:`DetectorCalibration` per series. This
module picks the winner from those calibrations.

The selector is read-only with respect to state: calibration write
ownership lives in :mod:`calibration` and :mod:`state`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Protocol

from .calibration import DetectorCalibration


class _PlanLike(Protocol):
    """Minimal shape the selector needs.

    Avoids importing :class:`_DetectorPlan` from runner so the
    dependency points one way (runner → selector).
    """

    name: str
    instance: str | None


CalibrationLookup = Callable[
    [str, str, tuple[tuple[str, str], ...], str, str | None],
    DetectorCalibration | None,
]


def select_winner(
    *,
    group_name: str,
    query_id: str,
    series_label_key: tuple[tuple[str, str], ...],
    plans: Sequence[_PlanLike],
    get_calibration: CalibrationLookup,
) -> tuple[str, str | None] | None:
    """Return ``(name, instance)`` of the highest-F1 detector, or None.

    Ranking: F1 desc, precision desc, config order asc (lower index
    wins). ``None`` is returned only when no plan has a stored
    calibration yet (warm-up window).
    """
    best: tuple[str, str | None] | None = None
    best_score: tuple[float, float, int] | None = None
    for idx, plan in enumerate(plans):
        cal = get_calibration(group_name, query_id, series_label_key, plan.name, plan.instance)
        if cal is None:
            continue
        # Higher idx loses on ties because we want config order to win;
        # negate so the tuple comparison ranks idx ascending.
        score_key = (cal.f1, cal.precision, -idx)
        if best_score is None or score_key > best_score:
            best_score = score_key
            best = (plan.name, plan.instance)
    return best


__all__ = ["CalibrationLookup", "select_winner"]
