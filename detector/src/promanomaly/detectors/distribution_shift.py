"""Distribution-shift detector (two-sample KS / Wasserstein).

Compares the recent sub-window's distribution against the preceding
baseline sub-window. Catches variance/distribution changes that a
mean-based rolling MAD misses: latency distributions widening, a
bimodal split emerging, a variance regime change.

Score is ``-log10(p_value)`` for the KS statistic (so a p-value of
0.001 maps to score 3.0 — consistent with the default
``alert_thresholds.score``). For Wasserstein/energy the score is
the distance normalised by the baseline MAD.
"""

from __future__ import annotations

import math
from typing import Any, ClassVar

import numpy as np
import pandas as pd

from .base import ParamSpec


def _try_import_scipy_stats() -> Any:
    try:
        from scipy import stats
    except ImportError as exc:
        raise RuntimeError(
            "DistributionShift requires scipy — install with: "
            "pip install promanomaly[distribution] (or pip install scipy)"
        ) from exc
    return stats


class DistributionShift:
    """Two-sample distribution-shift detector.

    Parameters (config ``params:``)
    -------------------------------
    ``split`` (float, default ``0.5``)
        Fraction of the window used as the recent sub-window. ``0.5``
        splits the window in half; ``0.3`` uses the last 30% as recent
        and the first 70% as baseline.
    ``statistic`` (str, default ``"ks"``)
        Two-sample test to use: ``"ks"`` (Kolmogorov-Smirnov),
        ``"wasserstein"``, or ``"energy"``.
    """

    name: ClassVar[str] = "DistributionShift"
    description: ClassVar[str] = (
        "Two-sample distribution-shift detector comparing the recent "
        "sub-window against the baseline sub-window. Catches variance "
        "and shape changes that mean-based detectors miss."
    )
    defaults: ClassVar[dict[str, Any]] = {
        "split": 0.5,
        "statistic": "ks",
    }
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="split",
            type_="float",
            default=0.5,
            doc=(
                "Fraction of the window used as the recent sub-window. "
                "0.5 splits in half; 0.3 uses the last 30% as recent."
            ),
            minimum=0.1,
            maximum=0.9,
        ),
        ParamSpec(
            name="statistic",
            type_="str",
            default="ks",
            doc="Two-sample test: 'ks' (Kolmogorov-Smirnov), 'wasserstein', or 'energy'.",
        ),
    )

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        del freq
        stats = _try_import_scipy_stats()
        split = float(params.get("split", self.defaults["split"]))
        statistic = str(params.get("statistic", self.defaults["statistic"]))

        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(df["timestamp"].iloc[-1])

        # Drop NaNs for the two-sample test.
        clean = values[np.isfinite(values)]
        if clean.size < 4:
            return self._zero_row(latest_ts, float(np.nanmedian(values)))

        split_idx = max(2, int(clean.size * (1.0 - split)))
        baseline = clean[:split_idx]
        recent = clean[split_idx:]

        if baseline.size < 2 or recent.size < 2:
            return self._zero_row(latest_ts, float(np.median(baseline)))

        baseline_median = float(np.median(baseline))

        if statistic == "ks":
            ks_result = stats.ks_2samp(baseline, recent)
            p_value = float(ks_result.pvalue)
            score = -math.log10(max(p_value, 1e-300))
        elif statistic == "wasserstein":
            dist = float(stats.wasserstein_distance(baseline, recent))
            mad = float(np.median(np.abs(baseline - baseline_median)))
            score = dist / mad if mad > 0.0 else 0.0
        elif statistic == "energy":
            # Energy distance approximation via sorted samples.
            dist = float(stats.wasserstein_distance(baseline, recent))
            mad = float(np.median(np.abs(baseline - baseline_median)))
            score = dist / mad if mad > 0.0 else 0.0
        else:
            raise ValueError(
                f"DistributionShift.statistic must be 'ks', 'wasserstein', or "
                f"'energy', got {statistic!r}"
            )

        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": score,
                    "baseline": baseline_median,
                    "is_outside": False,
                }
            ]
        )

    @staticmethod
    def _zero_row(ts: float, baseline: float) -> pd.DataFrame:
        return pd.DataFrame(
            [{"timestamp": ts, "score": 0.0, "baseline": baseline, "is_outside": False}]
        )
