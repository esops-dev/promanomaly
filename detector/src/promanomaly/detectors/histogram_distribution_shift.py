"""Histogram distribution-shift detector.

Scores shifts in the distribution across Prometheus histogram buckets —
a p99 latency tail fattening while the median holds, a bimodal split
emerging — which scalar detectors on a single quantile miss entirely.

Consumes a DataFrame whose ``y`` column encodes the bucket CDF values
(cumulative counts normalised to [0, 1]) at each timestamp row, with a
``le`` column carrying the bucket boundary. The runner expands classic
``_bucket`` series into this shape before calling the detector.

For operators who just want "anomaly on p95", the simpler path is
``histogram_quantile()`` in the source query fed into a scalar detector.
This detector is specifically for *whole-distribution* shape shifts.

Score normalisation follows the same ``-log10(p_value)`` scheme as
:class:`DistributionShift` for the KS statistic so the shared
``alert_thresholds.score`` works consistently.
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
            "HistogramDistributionShift requires scipy — install with: "
            "pip install promanomaly[distribution] (or pip install scipy)"
        ) from exc
    return stats


class HistogramDistributionShift:
    """Histogram bucket distribution-shift detector.

    Parameters (config ``params:``)
    -------------------------------
    ``split`` (float, default ``0.5``)
        Fraction of timestamps used as the recent window. ``0.5``
        splits in half.
    ``statistic`` (str, default ``"ks"``)
        Two-sample test on the reconstructed CDFs: ``"ks"`` or
        ``"wasserstein"``.
    ``n_samples`` (int, default ``200``)
        Number of synthetic samples drawn from each CDF for the
        two-sample test. Higher = more stable p-values but slower.
    """

    name: ClassVar[str] = "HistogramDistributionShift"
    description: ClassVar[str] = (
        "Scores distribution shifts across Prometheus histogram buckets. "
        "Catches tail fattening, bimodal splits, and whole-distribution "
        "shape changes that single-quantile detectors miss."
    )
    defaults: ClassVar[dict[str, Any]] = {
        "split": 0.5,
        "statistic": "ks",
        "n_samples": 200,
    }
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="split",
            type_="float",
            default=0.5,
            doc="Fraction of timestamps used as the recent window.",
            minimum=0.1,
            maximum=0.9,
        ),
        ParamSpec(
            name="statistic",
            type_="str",
            default="ks",
            doc="Two-sample test on reconstructed CDFs: 'ks' or 'wasserstein'.",
        ),
        ParamSpec(
            name="n_samples",
            type_="int",
            default=200,
            doc=(
                "Number of synthetic samples drawn from each CDF for the "
                "two-sample test. Higher = more stable p-values."
            ),
            minimum=10,
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
        n_samples = int(params.get("n_samples", self.defaults["n_samples"]))

        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(df["timestamp"].iloc[-1])

        # For histogram data, the ``y`` column carries per-bucket
        # cumulative counts at each timestamp. If ``le`` is absent we
        # fall back to treating the single-valued series as a
        # distribution of point values (same as DistributionShift).
        if "le" in df.columns:
            score = self._score_histogram(df, split, statistic, n_samples, stats)
        else:
            # Fallback: treat as raw values, same as DistributionShift.
            clean = values[np.isfinite(values)]
            if clean.size < 4:
                return self._zero_row(latest_ts, float(np.nanmedian(values)))

            split_idx = max(2, int(clean.size * (1.0 - split)))
            baseline = clean[:split_idx]
            recent = clean[split_idx:]
            if baseline.size < 2 or recent.size < 2:
                return self._zero_row(latest_ts, float(np.median(baseline)))

            score = self._two_sample(baseline, recent, statistic, stats)

        baseline_val = float(np.nanmedian(values))
        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": score,
                    "baseline": baseline_val,
                    "is_outside": False,
                }
            ]
        )

    @staticmethod
    def _score_histogram(
        df: pd.DataFrame,
        split: float,
        statistic: str,
        n_samples: int,
        stats: Any,
    ) -> float:
        """Score distribution shift from histogram bucket data."""
        le_vals = df["le"].to_numpy(dtype=float, copy=False)
        y_vals = df["y"].to_numpy(dtype=float, copy=False)
        timestamps = df["timestamp"].to_numpy(dtype=float, copy=False)

        unique_ts = np.unique(timestamps)
        if unique_ts.size < 4:
            return 0.0

        split_idx = max(2, int(unique_ts.size * (1.0 - split)))
        baseline_ts = unique_ts[:split_idx]
        recent_ts = unique_ts[split_idx:]

        # Aggregate bucket counts per window.
        unique_le = np.sort(np.unique(le_vals[np.isfinite(le_vals)]))
        if unique_le.size < 2:
            return 0.0

        def _aggregate_cdf(ts_arr: np.ndarray[Any, Any]) -> np.ndarray[Any, Any]:
            # Vectorised membership test — O(n) via numpy hashing instead
            # of the O(n*m) Python list comprehension it replaces.
            mask = np.isin(timestamps, ts_arr)
            agg = np.zeros(unique_le.size)
            for i, le in enumerate(unique_le):
                bucket_mask = mask & (le_vals == le)
                agg[i] = float(np.nansum(y_vals[bucket_mask]))
            # Normalise to CDF.
            total = agg[-1]
            if total > 0:
                agg = agg / total
            return agg

        baseline_cdf = _aggregate_cdf(baseline_ts)
        recent_cdf = _aggregate_cdf(recent_ts)

        # Sample from each CDF for the two-sample test.
        rng = np.random.default_rng(42)
        baseline_samples = _sample_from_cdf(unique_le, baseline_cdf, n_samples, rng)
        recent_samples = _sample_from_cdf(unique_le, recent_cdf, n_samples, rng)

        if baseline_samples.size < 2 or recent_samples.size < 2:
            return 0.0

        return HistogramDistributionShift._two_sample(
            baseline_samples, recent_samples, statistic, stats
        )

    @staticmethod
    def _two_sample(
        baseline: np.ndarray[Any, Any],
        recent: np.ndarray[Any, Any],
        statistic: str,
        stats: Any,
    ) -> float:
        if statistic == "ks":
            ks_result = stats.ks_2samp(baseline, recent)
            p_value = float(ks_result.pvalue)
            return -math.log10(max(p_value, 1e-300))
        elif statistic == "wasserstein":
            dist = float(stats.wasserstein_distance(baseline, recent))
            mad = float(np.median(np.abs(baseline - float(np.median(baseline)))))
            return dist / mad if mad > 0.0 else 0.0
        else:
            raise ValueError(
                f"HistogramDistributionShift.statistic must be 'ks' or "
                f"'wasserstein', got {statistic!r}"
            )

    @staticmethod
    def _zero_row(ts: float, baseline: float) -> pd.DataFrame:
        return pd.DataFrame(
            [{"timestamp": ts, "score": 0.0, "baseline": baseline, "is_outside": False}]
        )


def _sample_from_cdf(
    boundaries: np.ndarray[Any, Any],
    cdf: np.ndarray[Any, Any],
    n: int,
    rng: np.random.Generator,
) -> np.ndarray[Any, Any]:
    """Draw ``n`` samples from a piecewise-linear CDF defined by bucket boundaries."""
    # Prepend 0 to CDF for interpolation.
    cdf_full = np.concatenate([[0.0], cdf])
    boundaries_full = np.concatenate([[boundaries[0]], boundaries])

    # Draw uniform samples and inverse-CDF transform.
    u = rng.uniform(0.0, 1.0, size=n)
    result: np.ndarray[Any, Any] = np.interp(u, cdf_full, boundaries_full).astype(float)
    return result
