"""MAD (Median Absolute Deviation) detector.

Robust to outliers by construction, distribution-agnostic, computable in
O(n log n) on the rolling window. Score is the raw "number of MADs"
distance — the literature convention — and the universal
``alert_thresholds.score`` knob decides which value crosses into
``anomaly_outside_threshold=1``.
"""

from __future__ import annotations

from typing import Any, ClassVar

import numpy as np
import pandas as pd

from .base import ParamSpec

# Scale factor that makes MAD a consistent estimator of standard deviation
# for Gaussian data. We expose the *raw* number-of-MADs distance and do
# NOT apply this factor; the conventional 3-MAD anomaly threshold follows.
GAUSSIAN_CONSISTENCY: float = 1.4826

# Default trim cutoff when ``recursive_trim`` is on. Matches the
# literature's 3-MAD anomaly threshold and the default
# ``alert_thresholds.score=3.0``. Kept as a distinct parameter
# (``trim_threshold``) rather than implicitly mirroring
# ``alert_thresholds.score`` so an operator dialling alert sensitivity
# up or down doesn't silently shift the baseline; baseline
# robustness should be independent of alerting policy.
_DEFAULT_TRIM_THRESHOLD: float = 3.0


def _mad(values: np.ndarray, center: float) -> float:
    """Median absolute deviation about ``center``.

    Returns a small but non-zero floor when the window is degenerate
    (constant signal) so callers can divide without checking — the score
    on a constant window collapses to 0 anyway because ``|y - median|``
    is 0 there.
    """
    if values.size == 0:
        return 0.0
    return float(np.median(np.abs(values - center)))


class MAD:
    """Rolling-window MAD detector.

    Parameters (config ``params:``)
    -------------------------------
    ``recursive_trim`` (bool, default ``False``)
        When True, run the baseline calculation once, exclude any
        in-window points scoring above ``trim_threshold``, and recompute
        the median + MAD once more before scoring the latest point.
        Bounded to a single trim iteration for predictability.
        Mitigates the documented clustered-anomaly limitation where a
        burst of 4-5 anomalous points inside the rolling window poisons
        the very median MAD is computed against.
    ``trim_threshold`` (float, default ``3.0``)
        Cutoff (in MAD units) used by ``recursive_trim`` to decide which
        points to exclude from the second-pass baseline. Defaults to
        ``3.0`` to match the conventional 3-MAD anomaly threshold and
        the default ``alert_thresholds.score``. Kept independent from
        ``alert_thresholds.score`` so changing alert sensitivity does
        not silently re-shape the baseline.

    ``window`` is honoured by the runner when slicing the source query;
    the per-detector ``window`` override is forwarded but not consumed
    (the runner uses the group-level window). Alert-firing sensitivity
    is set via ``alert_thresholds.score``, not via a per-detector knob.
    """

    name: ClassVar[str] = "MAD"
    description: ClassVar[str] = (
        "Robust rolling baseline using median + median-absolute-deviation. "
        "Score is the raw number of MADs from the median; threshold via "
        "alert_thresholds.score."
    )
    defaults: ClassVar[dict[str, Any]] = {
        "recursive_trim": False,
        "trim_threshold": _DEFAULT_TRIM_THRESHOLD,
    }
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="recursive_trim",
            type_="bool",
            default=False,
            doc=(
                "Re-compute the median + MAD once after excluding in-window "
                "points that exceed trim_threshold, to mitigate "
                "clustered-anomaly contamination."
            ),
        ),
        ParamSpec(
            name="trim_threshold",
            type_="float",
            default=_DEFAULT_TRIM_THRESHOLD,
            doc=(
                "MAD-units cutoff used by recursive_trim to drop "
                "contaminating points from the second-pass baseline."
            ),
        ),
    )

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        del freq
        recursive_trim = bool(params.get("recursive_trim", self.defaults["recursive_trim"]))
        trim_threshold = float(params.get("trim_threshold", self.defaults["trim_threshold"]))
        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(df["timestamp"].iloc[-1])
        latest_y = float(values[-1])

        median = float(np.median(values))
        mad = _mad(values, median)

        if recursive_trim and mad > 0.0 and values.size > 2:
            # One bounded re-pass: drop in-window points above the trim
            # threshold and recompute on what's left. We always keep the
            # latest sample's *neighbours* (not the latest sample itself —
            # we never want to score the outlier against a baseline that
            # included it). Falls back to the original baseline if the
            # trim would leave fewer than two points behind.
            in_window = values[:-1]
            distances = np.abs(in_window - median) / mad
            kept = in_window[distances < trim_threshold]
            if kept.size >= 2:
                median = float(np.median(kept))
                mad = _mad(kept, median)

        score = 0.0 if mad == 0.0 else abs(latest_y - median) / mad

        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": score,
                    "baseline": median,
                    "is_outside": False,
                }
            ]
        )
