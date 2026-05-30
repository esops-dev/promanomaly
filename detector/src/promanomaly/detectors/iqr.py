"""IQR (Tukey interquartile-range) detector.

Non-parametric, deliberately lighter on the tails than MAD: the IQR
spans the middle 50% of the window, so anomalies are scored by how far
outside ``[Q1 - k*IQR, Q3 + k*IQR]`` they fall. Pairs well with MAD in
an ensemble because the two have noticeably different sensitivity
profiles on heavy-tailed data.
"""

from __future__ import annotations

from typing import Any, ClassVar

import numpy as np
import pandas as pd

from .base import ParamSpec


class IQR:
    """Tukey IQR-band outlier detector.

    Parameters (config ``params:``)
    -------------------------------
    ``k`` (float, default ``1.5``)
        Multiplier for the IQR band. ``1.5`` is Tukey's classical
        "outlier" definition; ``3.0`` is "far outlier".

    Score is the normalised distance outside the band, in units of
    ``IQR`` — directly comparable in shape (though not in scale) to
    MAD/Hampel's "number of MADs". Threshold sensitivity is set via
    ``alert_thresholds.score``.
    """

    name: ClassVar[str] = "IQR"
    description: ClassVar[str] = (
        "Tukey interquartile-range detector. Flags samples outside "
        "[Q1 - k*IQR, Q3 + k*IQR]; score is the normalised distance "
        "outside the band."
    )
    defaults: ClassVar[dict[str, Any]] = {"k": 1.5}
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="k",
            type_="float",
            default=1.5,
            doc="Multiplier for the IQR band. 1.5 = classical outlier; 3.0 = far outlier.",
            minimum=0.0,
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
        try:
            k = float(k_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"IQR.k must be numeric, got {k_raw!r}") from exc
        if k <= 0:
            raise ValueError(f"IQR.k must be positive, got {k}")

        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(df["timestamp"].iloc[-1])
        latest_y = float(values[-1])

        q1 = float(np.quantile(values, 0.25))
        q3 = float(np.quantile(values, 0.75))
        iqr = q3 - q1
        median = float(np.median(values))

        if iqr == 0.0:
            # Degenerate window (constant or near-constant signal).
            # Score collapses to 0 — same convention as MAD/Hampel.
            score = 0.0
        else:
            lower = q1 - k * iqr
            upper = q3 + k * iqr
            if latest_y > upper:
                score = (latest_y - upper) / iqr
            elif latest_y < lower:
                score = (lower - latest_y) / iqr
            else:
                score = 0.0

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
