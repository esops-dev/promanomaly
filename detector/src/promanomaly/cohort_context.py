"""Pre-computed per-cohort baselines for cohort-aware detectors.

The Cohort detector scores each series against the cross-member median
+ MAD of its cohort, not against its own history. To avoid recomputing
that baseline once per series, the runner builds a
:class:`CohortContext` once per query run and threads it through
scoring; every member of a cohort then sees the same baseline.

The dataclasses are frozen because the runner threads them through
several layers and accidental mutation would silently corrupt
subsequent members' scores.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from .detectors.cohort import (
    cohort_key_for,
    compute_cohort_baseline,
    is_cohort_aware,
)
from .plan import DetectorPlan
from .source import QuerySeries


@dataclass(frozen=True)
class CohortBaseline:
    """Per-cohort cross-member median + MAD precomputed by the runner.

    Built once per ``(plan, cohort_key)`` before scoring loops over the
    cohort's members so every member sees the same baseline.
    """

    cohort_median: float
    cohort_mad: float
    size: int


@dataclass
class CohortContext:
    """All cohort baselines for one query run, indexed by plan + cohort key.

    Cohort-aware plans look up their per-series baseline via
    ``baselines[(plan.name, plan.instance), cohort_key]``.
    ``label_by_plan`` records the resolved ``cohort_label`` per plan so
    the runner can stamp the opt-in ``cohort_label`` output label on
    emitted samples.
    """

    baselines: dict[
        tuple[tuple[str, str | None], tuple[tuple[str, str], ...]],
        CohortBaseline,
    ] = field(default_factory=dict)
    label_by_plan: dict[tuple[str, str | None], str] = field(default_factory=dict)


def build_cohort_context(
    plans: Sequence[DetectorPlan],
    series_list: list[QuerySeries],
) -> CohortContext:
    """Pre-compute cohort baselines for every cohort-aware plan.

    For each cohort-aware plan, partitions ``series_list`` by
    ``cohort_key`` (all labels except the configured ``cohort_label``)
    and computes the cross-member median + MAD for each cohort that
    meets ``min_cohort_size``. Cohorts below the floor produce no
    entry — the Cohort detector observes the absence and emits a
    zero-score row, which the runner forwards as ``anomaly_score=0``.

    Costs are bounded by ``safety.max_series_per_query``; on a 1000-
    series query the inner numpy work is microseconds.
    """
    context = CohortContext()
    if not series_list:
        return context

    for plan in plans:
        if not is_cohort_aware(plan.cls):
            continue
        cohort_label = str(
            plan.params.get(
                "cohort_label",
                getattr(plan.cls, "defaults", {}).get("cohort_label", "instance"),
            )
        )
        try:
            min_size = int(
                plan.params.get(
                    "min_cohort_size",
                    getattr(plan.cls, "defaults", {}).get("min_cohort_size", 5),
                )
            )
        except (TypeError, ValueError):
            min_size = 5
        context.label_by_plan[(plan.name, plan.instance)] = cohort_label

        # Partition series by cohort key.
        buckets: dict[tuple[tuple[str, str], ...], list[np.ndarray[Any, Any]]] = {}
        for series in series_list:
            if not series.samples.shape[0]:
                continue
            key = cohort_key_for(series.labels, cohort_label)
            values = series.samples["y"].to_numpy(dtype=float, copy=False)
            buckets.setdefault(key, []).append(values)

        for cohort_key, member_arrays in buckets.items():
            if len(member_arrays) < min_size:
                continue
            baseline = compute_cohort_baseline(member_arrays)
            if baseline is None:
                continue
            cohort_median, cohort_mad = baseline
            context.baselines[((plan.name, plan.instance), cohort_key)] = CohortBaseline(
                cohort_median=cohort_median,
                cohort_mad=cohort_mad,
                size=len(member_arrays),
            )
    return context


__all__ = ["CohortBaseline", "CohortContext", "build_cohort_context"]
