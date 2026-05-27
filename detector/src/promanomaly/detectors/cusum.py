"""Classical CUSUM (cumulative sum) change-point detector.

Tracks the cumulative deviation of the signal from a robust baseline,
firing when the running sum crosses a configurable threshold. Faster and
simpler than BOCPD, with a single interpretable knob (``h`` — decision
threshold in sigma units) on top of the optional reference value ``k``.

The MAD-derived sigma estimator keeps the threshold meaningful on
heavy-tailed signals where ``np.std`` would balloon after the first
spike. This is the same convention used elsewhere in promanomaly — the
"number of MADs from baseline" units carry across detectors.
"""

from __future__ import annotations

from typing import Any, ClassVar

import numpy as np
import pandas as pd

from .base import ParamSpec

# Scaling factor that converts MAD to a sigma estimate for Gaussian data.
# We use it to keep CUSUM's ``h`` interpretable as "sigma multiplier"
# regardless of the baseline detector.
GAUSSIAN_CONSISTENCY: float = 1.4826


class CUSUM:
    """Two-sided CUSUM detector with MAD-derived sigma.

    Parameters (config ``params:``)
    -------------------------------
    ``k`` (float, default ``0.5``)
        Reference value (in sigma units). Slack subtracted from each
        deviation before it accumulates; controls how large a deviation
        must be before CUSUM starts to grow. 0.5 sigma is the
        textbook starting point.
    ``h`` (float, default ``5.0``)
        Decision threshold (in sigma units). A change-point fires when
        either the upper or lower CUSUM crosses ``h * sigma`` at the
        latest sample. Larger values are more conservative; 4-5 sigma
        is the conventional range.

    Output columns
    --------------
    - ``score``: current CUSUM magnitude (``max(|S+|, |S-|) / sigma``),
      in sigma units. Same scale as MAD/Hampel scores, so the global
      ``alert_thresholds.score`` knob works the same way.
    - ``baseline``: rolling-window median.
    - ``is_outside``: ``True`` when ``score >= alert_thresholds.score``
      (handled by the runner) — the standard outlier alert.
    - ``change_point``: ``True`` when the CUSUM *crosses* ``h * sigma``
      at the latest sample (i.e. fresh fire, not residual from an
      earlier crossing). Feeds ``anomaly_change_point_total``.
    """

    name: ClassVar[str] = "CUSUM"
    description: ClassVar[str] = (
        "Classical CUSUM change-point detector. Tracks cumulative deviations "
        "from a robust baseline; flags a change-point when either tail of "
        "the running sum crosses h sigma at the latest sample."
    )
    defaults: ClassVar[dict[str, Any]] = {"k": 0.5, "h": 5.0}
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="k",
            type_="float",
            default=0.5,
            doc="Reference value in sigma units. Slack subtracted before accumulation.",
        ),
        ParamSpec(
            name="h",
            type_="float",
            default=5.0,
            doc="Decision threshold in sigma units; change-point fires at h*sigma.",
        ),
    )

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        del freq
        k_raw = params.get("k", self.defaults["k"])
        h_raw = params.get("h", self.defaults["h"])
        try:
            k = float(k_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"CUSUM.k must be numeric, got {k_raw!r}") from exc
        try:
            h = float(h_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"CUSUM.h must be numeric, got {h_raw!r}") from exc
        if k < 0:
            raise ValueError(f"CUSUM.k must be non-negative, got {k}")
        if h <= 0:
            raise ValueError(f"CUSUM.h must be positive, got {h}")

        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(df["timestamp"].iloc[-1])

        median = float(np.median(values))
        mad = float(np.median(np.abs(values - median)))
        # Use MAD * 1.4826 as the sigma estimate when MAD > 0; fall back
        # to numpy std for truly constant windows so CUSUM degrades to
        # "do nothing" rather than divide-by-zero.
        sigma = mad * GAUSSIAN_CONSISTENCY if mad > 0.0 else max(float(np.std(values)), 1e-12)

        slack = k * sigma
        threshold = h * sigma

        s_high = 0.0
        s_low = 0.0
        prev_max = 0.0
        change_point_at_latest = False

        n = values.size
        for i in range(n):
            x = float(values[i])
            new_s_high = max(0.0, s_high + (x - median) - slack)
            new_s_low = min(0.0, s_low + (x - median) + slack)
            new_max = max(new_s_high, abs(new_s_low))
            is_last = i == n - 1
            if is_last:
                # Detect a *fresh* crossing at the latest sample. The
                # prev_max guard prevents repeatedly counting the same
                # event on successive overlapping rolling-window calls.
                if new_max >= threshold and prev_max < threshold:
                    change_point_at_latest = True
                s_high = new_s_high
                s_low = new_s_low
            else:
                # Reset the CUSUM after an earlier crossing so a single
                # past change-point doesn't permanently inflate the
                # statistic for the rest of the window.
                if new_max >= threshold:
                    s_high = 0.0
                    s_low = 0.0
                    prev_max = 0.0
                else:
                    s_high = new_s_high
                    s_low = new_s_low
                    prev_max = new_max

        score = max(s_high, abs(s_low)) / sigma

        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": float(score),
                    "baseline": median,
                    "is_outside": False,
                    "change_point": bool(change_point_at_latest),
                }
            ]
        )
