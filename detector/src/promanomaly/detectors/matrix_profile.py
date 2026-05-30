"""Matrix-profile (discord) shape detector.

Detects anomalies that are *wrong shapes* — a missing daily ramp, an
irregular waveform — rather than level or point outliers. Computes the
matrix profile over the rolling window at a configurable subsequence
length ``m``; the latest subsequence's distance to its nearest
neighbour (the "discord" distance) is the score, normalised by the
mean profile distance.

Requires ``stumpy`` at runtime — imported lazily so the module loads
cleanly even when stumpy isn't installed. Given the heavier compute
and dependency, this detector ships as an optional extra. Install with:
``pip install promanomaly[matrixprofile]`` (or ``pip install stumpy``).
"""

from __future__ import annotations

from typing import Any, ClassVar

import numpy as np
import pandas as pd

from .base import ParamSpec


def _try_import_stumpy() -> Any:
    """Resolve stumpy at call time."""
    try:
        import stumpy
    except ImportError as exc:
        raise RuntimeError(
            "MatrixProfile requires stumpy — install with: "
            "pip install promanomaly[matrixprofile] (or pip install stumpy)"
        ) from exc
    return stumpy


class MatrixProfile:
    """Matrix-profile discord detector.

    Parameters (config ``params:``)
    -------------------------------
    ``m`` (int, default ``24``)
        Subsequence length for the matrix profile. Should roughly
        match one cycle of the expected pattern (e.g. 24 for hourly
        samples over a daily cycle).
    """

    name: ClassVar[str] = "MatrixProfile"
    description: ClassVar[str] = (
        "Matrix-profile discord detector. Finds wrong-shape anomalies "
        "(missing ramps, irregular waveforms) by scoring the latest "
        "subsequence's nearest-neighbour distance."
    )
    defaults: ClassVar[dict[str, Any]] = {
        "m": 24,
    }
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="m",
            type_="int",
            default=24,
            doc=(
                "Subsequence length. Should roughly match one cycle of the "
                "expected pattern (e.g. 24 for hourly on daily)."
            ),
            minimum=3,
        ),
    )

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        del freq
        stumpy = _try_import_stumpy()
        m = int(params.get("m", self.defaults["m"]))

        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(df["timestamp"].iloc[-1])
        baseline = float(np.nanmedian(values))

        # Matrix profile requires at least 2*m data points.
        if values.size < 2 * m:
            return pd.DataFrame(
                [
                    {
                        "timestamp": latest_ts,
                        "score": 0.0,
                        "baseline": baseline,
                        "is_outside": False,
                    }
                ]
            )

        # Fill NaNs for stumpy (it can't handle them).
        clean = values.copy()
        nan_mask = ~np.isfinite(clean)
        if nan_mask.any():
            clean[nan_mask] = float(np.nanmedian(values))

        profile = stumpy.stump(clean, m)
        # profile[:, 0] contains the nearest-neighbour distances.
        nn_distances = profile[:, 0].astype(float)

        # The latest subsequence is the last entry in the profile.
        latest_dist = float(nn_distances[-1])

        # Normalise by the mean NN distance across the profile.
        finite_dists = nn_distances[np.isfinite(nn_distances)]
        if finite_dists.size < 2:
            score = 0.0
        else:
            median_dist = float(np.median(finite_dists))
            mad_dist = float(np.median(np.abs(finite_dists - median_dist)))
            deviation = latest_dist - median_dist
            if mad_dist > 0.0:
                score = deviation / mad_dist
            elif deviation > 0.0:
                # Degenerate case: most NN distances are identical.
                # Fall back to std as the denominator.
                std_dist = float(np.std(finite_dists))
                score = deviation / std_dist if std_dist > 0.0 else 0.0
            else:
                score = 0.0
            # Clamp to non-negative: a latest distance close to the
            # median is normal, not anomalous.
            score = max(score, 0.0)

        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": score,
                    "baseline": baseline,
                    "is_outside": False,
                    "anomaly_type": "shape_discord",
                }
            ]
        )
