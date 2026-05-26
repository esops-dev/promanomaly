"""Hampel filter detector.

Like MAD, but the median and MAD are computed over a *local* sub-window
of width ``2*t0 + 1`` centred on the point being scored — which copes
better when the baseline drifts within the rolling window.
"""

from __future__ import annotations

from typing import Any, ClassVar

import numpy as np
import pandas as pd


class Hampel:
    """Hampel filter detector.

    Parameters (config ``params:``)
    -------------------------------
    ``t0`` (int, default ``3``)
        Half-width of the local-context sub-window. The actual sub-window
        spans ``2 * t0 + 1`` points centred on the target.

    Threshold sensitivity is set via ``alert_thresholds.score``, not via
    a per-detector ``k`` knob.
    """

    name: ClassVar[str] = "Hampel"
    description: ClassVar[str] = (
        "Local-window robust outlier filter. Score is |y - local_median| / "
        "local_MAD, same units as the MAD detector for cross-detector "
        "comparison. Better when the baseline drifts within the window."
    )
    defaults: ClassVar[dict[str, Any]] = {"t0": 3}

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        del freq
        t0_raw = params.get("t0", self.defaults["t0"])
        try:
            t0 = max(1, int(t0_raw))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Hampel.t0 must be an integer, got {t0_raw!r}") from exc

        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(df["timestamp"].iloc[-1])
        latest_y = float(values[-1])

        # Local sub-window: the last ``2*t0 + 1`` points if available, else
        # everything we've got. Centring on the latest point is unnecessary
        # here because the latest point is at the right edge of the rolling
        # window — there are no future samples to include.
        sub_window_size = 2 * t0 + 1
        start = max(0, values.size - sub_window_size)
        local = values[start:]

        local_median = float(np.median(local))
        local_mad = float(np.median(np.abs(local - local_median)))

        score = 0.0 if local_mad == 0.0 else abs(latest_y - local_median) / local_mad

        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": score,
                    "baseline": local_median,
                    "is_outside": False,
                }
            ]
        )
