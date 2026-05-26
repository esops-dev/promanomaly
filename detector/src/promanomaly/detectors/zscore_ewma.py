"""Z-score detector with EWMA mean and variance.

Designed for roughly-Gaussian signals with a slowly drifting mean: cache
hit rate, sustained throughput, slowly growing capacity. The EWMA tracks
the drift so a long-term shift doesn't smear the baseline the way a
fixed-window mean does.

NOT for heavy-tailed traffic data — see ``docs/detectors.md`` for the
counter-example. Use MAD or Hampel there.
"""

from __future__ import annotations

import math
from typing import Any, ClassVar

import numpy as np
import pandas as pd

from .base import ParamSpec


def _ewma_mean_var(values: np.ndarray, alpha: float) -> tuple[float, float]:
    """Compute EWMA mean and EWMA variance over ``values``.

    Uses the standard incremental update so the result matches what an
    online process would converge to without keeping all samples in
    memory at once. Returns ``(mean, variance)`` for the final point.
    """
    mean = float(values[0])
    var = 0.0
    for x in values[1:]:
        diff = float(x) - mean
        incr = alpha * diff
        mean = mean + incr
        # Welford-style EWMA variance: the conventional form used in
        # control charting; biased estimator but stable for small alpha.
        var = (1.0 - alpha) * (var + diff * incr)
    return mean, var


class ZScoreEWMA:
    """EWMA-baseline z-score detector.

    Parameters (config ``params:``)
    -------------------------------
    ``alpha`` (float, default ``0.05``)
        EWMA smoothing factor. Smaller values track the mean more
        slowly; larger values adapt faster but let recent anomalies
        pollute the baseline more.

    Threshold sensitivity is set via ``alert_thresholds.score``, not via
    a per-detector knob.
    """

    name: ClassVar[str] = "ZScoreEWMA"
    description: ClassVar[str] = (
        "Z-score against an exponentially-weighted moving mean and variance. "
        "For roughly-Gaussian signals with slowly drifting baselines."
    )
    defaults: ClassVar[dict[str, Any]] = {"alpha": 0.05}
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="alpha",
            type_="float",
            default=0.05,
            doc=(
                "EWMA smoothing factor in (0, 1]. Smaller tracks the mean more slowly; "
                "larger adapts faster but lets recent anomalies pollute the baseline."
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
        alpha_raw = params.get("alpha", self.defaults["alpha"])
        try:
            alpha = float(alpha_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"ZScoreEWMA.alpha must be numeric, got {alpha_raw!r}") from exc
        if not (0.0 < alpha <= 1.0):
            raise ValueError(f"ZScoreEWMA.alpha must be in (0, 1], got {alpha}")

        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(df["timestamp"].iloc[-1])
        latest_y = float(values[-1])

        if values.size == 1:
            mean, var = float(values[0]), 0.0
        else:
            # Exclude the latest sample from the baseline so we score
            # it against history, not against itself.
            mean, var = _ewma_mean_var(values[:-1], alpha)

        std = math.sqrt(var)
        score = 0.0 if std == 0.0 else abs(latest_y - mean) / std

        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": score,
                    "baseline": mean,
                    "is_outside": False,
                }
            ]
        )
