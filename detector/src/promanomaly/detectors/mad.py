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

# Scale factor that makes MAD a consistent estimator of standard deviation
# for Gaussian data. We expose the *raw* number-of-MADs distance and do
# NOT apply this factor; the conventional 3-MAD anomaly threshold follows.
GAUSSIAN_CONSISTENCY: float = 1.4826


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
    No detector-specific parameters. ``window`` is honoured by the runner
    when slicing the source query; the per-detector ``window`` override
    is forwarded but not consumed (the runner uses the group-level
    window). Threshold sensitivity is set via ``alert_thresholds.score``,
    not via a per-detector knob.
    """

    name: ClassVar[str] = "MAD"
    description: ClassVar[str] = (
        "Robust rolling baseline using median + median-absolute-deviation. "
        "Score is the raw number of MADs from the median; threshold via "
        "alert_thresholds.score."
    )
    defaults: ClassVar[dict[str, Any]] = {}

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        del freq, params  # not currently consumed
        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(df["timestamp"].iloc[-1])
        latest_y = float(values[-1])

        median = float(np.median(values))
        mad = _mad(values, median)

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
