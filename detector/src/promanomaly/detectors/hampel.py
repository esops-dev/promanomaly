"""Hampel filter detector.

Like MAD, but the median and MAD are computed over a *local* sub-window
of width ``2*t0 + 1`` centred on the point being scored — which copes
better when the baseline drifts within the rolling window.
"""

from __future__ import annotations

from typing import Any, ClassVar

import numpy as np
import pandas as pd

from .base import ParamSpec

# Default trim cutoff when ``recursive_trim`` is on. Matches the MAD
# detector's default so the two agree on what "anomalous" means when
# trimming, and intentionally distinct from ``alert_thresholds.score``
# so baseline robustness doesn't shift when alert sensitivity does.
_DEFAULT_TRIM_THRESHOLD: float = 3.0


class Hampel:
    """Hampel filter detector.

    Parameters (config ``params:``)
    -------------------------------
    ``t0`` (int, default ``3``)
        Half-width of the local-context sub-window. The actual sub-window
        spans ``2 * t0 + 1`` points centred on the target.
    ``recursive_trim`` (bool, default ``False``)
        When True, recompute the local median + MAD once after excluding
        sub-window points that exceed ``trim_threshold``, then score the
        latest point against the trimmed baseline. Bounded to one trim
        pass for predictability.
    ``trim_threshold`` (float, default ``3.0``)
        Local-MAD-units cutoff used by ``recursive_trim``. Same default
        and decoupling rationale as the MAD detector.

    Alert-firing sensitivity is set via ``alert_thresholds.score``, not
    via a per-detector ``k`` knob.
    """

    name: ClassVar[str] = "Hampel"
    description: ClassVar[str] = (
        "Local-window robust outlier filter. Score is |y - local_median| / "
        "local_MAD, same units as the MAD detector for cross-detector "
        "comparison. Better when the baseline drifts within the window."
    )
    defaults: ClassVar[dict[str, Any]] = {
        "t0": 3,
        "recursive_trim": False,
        "trim_threshold": _DEFAULT_TRIM_THRESHOLD,
    }
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="t0",
            type_="int",
            default=3,
            doc="Half-width of the local sub-window; sub-window spans 2*t0 + 1 points.",
        ),
        ParamSpec(
            name="recursive_trim",
            type_="bool",
            default=False,
            doc=(
                "Re-compute the local median + MAD once after excluding "
                "sub-window points that exceed trim_threshold, to "
                "mitigate clustered-anomaly contamination."
            ),
        ),
        ParamSpec(
            name="trim_threshold",
            type_="float",
            default=_DEFAULT_TRIM_THRESHOLD,
            doc=(
                "Local-MAD-units cutoff used by recursive_trim to drop "
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
        t0_raw = params.get("t0", self.defaults["t0"])
        try:
            t0 = max(1, int(t0_raw))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Hampel.t0 must be an integer, got {t0_raw!r}") from exc
        recursive_trim = bool(params.get("recursive_trim", self.defaults["recursive_trim"]))
        trim_threshold = float(params.get("trim_threshold", self.defaults["trim_threshold"]))

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

        if recursive_trim and local_mad > 0.0 and local.size > 2:
            # Strip the latest sample (the one we're about to score) and
            # any prior points beyond the trim threshold, then recompute.
            # Bounded to a single pass for predictability; matches the
            # MAD detector's contract.
            neighbours = local[:-1]
            distances = np.abs(neighbours - local_median) / local_mad
            kept = neighbours[distances < trim_threshold]
            if kept.size >= 2:
                local_median = float(np.median(kept))
                local_mad = float(np.median(np.abs(kept - local_median)))

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
