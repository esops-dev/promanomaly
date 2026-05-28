"""Cohort comparison detector — flags fleet members that diverge from peers.

Unlike every other detector in this package, the Cohort detector needs to
see the whole population at once. A single series scored in isolation
would always look "normal" because its own rolling window is the
baseline; the anomaly only appears when the series is compared against
its peers. The detector exposes ``cohort_aware = True`` so the runner
pre-computes the cross-cohort baseline once per query and stamps the
result into ``params`` before each ``fit_score`` call. ``fit_score``
itself stays single-series-shaped, so the Detector Protocol does not
have to bifurcate.

Scope is deliberate: this is **fleet-relative** anomaly detection, not
multivariate / covariance-shift / copula analysis. The latter is a
future-considerations item in the roadmap; cohort comparison is the
restricted, well-understood version that catches the canonical "one
bad node in the fleet" case without dragging in stateful tuning.
"""

from __future__ import annotations

from typing import Any, ClassVar

import numpy as np
import pandas as pd

from .base import ParamSpec

_DEFAULT_COHORT_LABEL: str = "instance"
_DEFAULT_MIN_COHORT_SIZE: int = 5

# Internal params keys the runner stamps onto ``plan.params`` before each
# ``fit_score`` call. Underscored so they cannot collide with user-facing
# config knobs and are easy to grep for from the runner side.
_PARAM_COHORT_MEDIAN: str = "_cohort_baseline_median"
_PARAM_COHORT_MAD: str = "_cohort_baseline_mad"
_PARAM_COHORT_SIZE: str = "_cohort_size"


class Cohort:
    """Per-query cross-member baseline detector.

    The runner groups series sharing every label *except* ``cohort_label``
    and feeds the per-cohort median + MAD here. The score for one
    member is::

        score = |rolling_mean(member) - cohort_median| / cohort_mad

    where ``cohort_median`` and ``cohort_mad`` are themselves rolling
    summaries over the window — taken as the median across time of the
    per-step cross-member median and MAD respectively, which keeps the
    baseline insensitive to individual members shifting briefly.

    Cohorts with fewer than ``min_cohort_size`` members are skipped:
    no sample is emitted for the member, on the operator-friendly
    "two-node cluster" theory that a tiny cohort cannot meaningfully
    median against anything.

    Parameters (config ``params:``)
    -------------------------------
    ``cohort_label`` (str, default ``"instance"``)
        The label that varies between cohort members. Every other label
        on the series identifies the cohort itself. Common values:
        ``instance``, ``pod``, ``replica``.
    ``min_cohort_size`` (int, default ``5``)
        Skip scoring when a cohort has fewer than this many members.
        Guards against ``MAD == 0`` collapses and the degenerate
        two-member case where one member is the cohort's own median.
    """

    name: ClassVar[str] = "Cohort"
    description: ClassVar[str] = (
        "Cross-cohort baseline detector. Groups series sharing all labels "
        "except cohort_label; scores each member by deviation of its rolling "
        "mean from the cross-member median, in units of cross-member MAD."
    )
    defaults: ClassVar[dict[str, Any]] = {
        "cohort_label": _DEFAULT_COHORT_LABEL,
        "min_cohort_size": _DEFAULT_MIN_COHORT_SIZE,
    }
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="cohort_label",
            type_="str",
            default=_DEFAULT_COHORT_LABEL,
            doc=(
                "Label that varies between cohort members; every other "
                "source label identifies the cohort itself."
            ),
        ),
        ParamSpec(
            name="min_cohort_size",
            type_="int",
            default=_DEFAULT_MIN_COHORT_SIZE,
            doc=(
                "Minimum cohort members required before scoring runs; "
                "smaller cohorts emit no sample for their members."
            ),
        ),
    )

    # The runner reads this class attribute to decide whether to pre-build
    # the cohort context for a query. The protocol stays single-series.
    cohort_aware: ClassVar[bool] = True

    @classmethod
    def cohort_label_from(cls, params: dict[str, Any]) -> str:
        """Resolve the configured cohort_label, falling back to the default."""
        value = params.get("cohort_label", cls.defaults["cohort_label"])
        return str(value)

    @classmethod
    def min_cohort_size_from(cls, params: dict[str, Any]) -> int:
        try:
            return int(params.get("min_cohort_size", cls.defaults["min_cohort_size"]))
        except (TypeError, ValueError):
            return _DEFAULT_MIN_COHORT_SIZE

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        del freq
        values = df["y"].to_numpy(dtype=float, copy=False)
        timestamps = df["timestamp"].to_numpy(dtype=float, copy=False)
        latest_ts = float(timestamps[-1]) if timestamps.size else 0.0

        cohort_median_raw = params.get(_PARAM_COHORT_MEDIAN)
        cohort_mad_raw = params.get(_PARAM_COHORT_MAD)
        cohort_size_raw = params.get(_PARAM_COHORT_SIZE)
        min_size = self.min_cohort_size_from(params)

        # Insufficient cohort: emit a zero-score row with a NaN-safe
        # baseline so the runner doesn't have to special-case the skip.
        # ``anomaly_outside_threshold`` will stay 0 because score is 0.
        if (
            cohort_median_raw is None
            or cohort_mad_raw is None
            or cohort_size_raw is None
            or int(cohort_size_raw) < min_size
        ):
            return pd.DataFrame(
                [
                    {
                        "timestamp": latest_ts,
                        "score": 0.0,
                        "baseline": float(np.mean(values)) if values.size else 0.0,
                        "is_outside": False,
                    }
                ]
            )

        cohort_median = float(cohort_median_raw)
        cohort_mad = float(cohort_mad_raw)
        rolling_mean = float(np.mean(values)) if values.size else 0.0

        # Constant fleet (cohort_mad == 0): nothing to median-against,
        # so the score collapses to 0 the same way MAD does on a
        # flatline rather than producing a NaN via division-by-zero.
        score = 0.0 if cohort_mad == 0.0 else abs(rolling_mean - cohort_median) / cohort_mad

        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": score,
                    "baseline": cohort_median,
                    "is_outside": False,
                }
            ]
        )


def cohort_key_for(labels: dict[str, str], cohort_label: str) -> tuple[tuple[str, str], ...]:
    """Compute the hashable cohort identity from one series' label set.

    All labels *other than* ``cohort_label`` are part of the cohort key —
    two series sharing every non-cohort label belong to the same cohort.
    Sorted tuple so the order in which labels arrive doesn't influence
    the identity.
    """
    return tuple(sorted((k, v) for k, v in labels.items() if k != cohort_label))


def is_cohort_aware(cls: type[Any]) -> bool:
    """Whether a detector class opts into the cohort-context pre-pass."""
    return bool(getattr(cls, "cohort_aware", False))


def compute_cohort_baseline(
    cohort_values: list[np.ndarray],
) -> tuple[float, float] | None:
    """Compute ``(cross_member_median, cross_member_mad)`` for one cohort.

    Each entry in ``cohort_values`` is the per-step value array of one
    member, all aligned by index (the runner guarantees this — cohort
    members share the PromQL fetch). Returns ``None`` when there is no
    usable data — caller treats that as "skip this cohort".

    Summarisation: per-step cross-member median + MAD over time, then
    median over time of each. The median-of-medians keeps the baseline
    insensitive to short-lived spikes in one member; the median-of-MADs
    avoids letting one volatile sample push the cohort scale around.
    """
    if not cohort_values:
        return None
    # Align lengths: members may have slightly different counts if the
    # TSDB returned partial coverage. Trim to the shortest length so
    # numpy can stack into a 2D array.
    min_len = min(arr.size for arr in cohort_values)
    if min_len == 0:
        return None
    trimmed = np.stack([arr[-min_len:] for arr in cohort_values], axis=0)
    # axis=0 reduces across members; result is one number per timestep.
    per_step_median = np.median(trimmed, axis=0)
    per_step_mad = np.median(np.abs(trimmed - per_step_median[np.newaxis, :]), axis=0)
    cohort_median = float(np.median(per_step_median))
    cohort_mad = float(np.median(per_step_mad))
    return cohort_median, cohort_mad


__all__ = [
    "_PARAM_COHORT_MAD",
    "_PARAM_COHORT_MEDIAN",
    "_PARAM_COHORT_SIZE",
    "Cohort",
    "cohort_key_for",
    "compute_cohort_baseline",
    "is_cohort_aware",
]
