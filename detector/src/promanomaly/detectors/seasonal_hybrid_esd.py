"""Seasonal-Hybrid ESD (S-H-ESD) detector.

Median-based seasonal decomposition plus generalised Extreme
Studentized Deviate (ESD) on the residual. A well-trodden, robust
seasonal-anomaly method for strongly seasonal signals with occasional
outliers.

This is still baseline detection, not forecasting: it scores residuals
against historical structure, it does not predict future values.

Score is the ratio of the ESD test statistic to the critical value —
values above 1.0 indicate the residual is an outlier at the configured
significance level. Normalised so the default
``alert_thresholds.score=3.0`` fires on clearly significant outliers.
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
            "SeasonalHybridESD requires scipy — install with: "
            "pip install promanomaly[distribution] (or pip install scipy)"
        ) from exc
    return stats


def _median_seasonal_decompose(
    values: np.ndarray[Any, Any], period: int
) -> tuple[np.ndarray[Any, Any], np.ndarray[Any, Any]]:
    """Simple median-based seasonal decomposition.

    Returns ``(seasonal, residual)`` arrays. The seasonal component is
    the per-bucket median repeated across the series; the residual is
    ``values - seasonal``.
    """
    n = values.size
    seasonal = np.zeros(n)
    for i in range(period):
        bucket_indices = np.arange(i, n, period)
        bucket_values = values[bucket_indices]
        finite = bucket_values[np.isfinite(bucket_values)]
        bucket_median = float(np.median(finite)) if finite.size > 0 else 0.0
        seasonal[bucket_indices] = bucket_median
    residual = values - seasonal
    return seasonal, residual


def _generalized_esd(
    residuals: np.ndarray[Any, Any],
    max_anomalies: int,
    alpha: float,
    stats_mod: Any,
) -> tuple[float, bool]:
    """Generalised ESD test on residuals.

    Returns ``(score, is_outlier)`` for the latest sample (last element
    of ``residuals``). The score is the ratio of the test statistic to
    the critical value for the latest point; values > 1.0 mean the
    residual is an outlier at the ``alpha`` significance level.
    """
    clean = residuals[np.isfinite(residuals)]
    if clean.size < 3:
        return 0.0, False

    # Track whether the latest sample is flagged.
    n = clean.size
    latest_idx = n - 1
    indices = np.arange(n)
    latest_flagged = False
    latest_score = 0.0

    working = clean.copy()
    working_indices = indices.copy()

    for k in range(1, min(max_anomalies, n - 2) + 1):
        median = float(np.median(working))
        mad = float(np.median(np.abs(working - median)))
        if mad == 0.0:
            mad = float(np.std(working))
        if mad == 0.0:
            break

        deviations = np.abs(working - median) / mad
        max_pos = int(np.argmax(deviations))
        test_stat = float(deviations[max_pos])

        # Critical value from t-distribution.
        p = 1.0 - alpha / (2.0 * (n - k + 1))
        p = min(max(p, 0.5 + 1e-10), 1.0 - 1e-10)
        t_val = float(stats_mod.t.ppf(p, n - k - 1)) if n - k - 1 > 0 else 0.0
        critical = (
            (n - k) * t_val / math.sqrt((n - k - 1 + t_val**2) * (n - k + 1))
            if n - k - 1 > 0
            else 0.0
        )

        if critical > 0.0 and working_indices[max_pos] == latest_idx:
            latest_score = test_stat / critical
            if test_stat > critical:
                latest_flagged = True

        # Remove the most extreme point and continue.
        mask = np.ones(working.size, dtype=bool)
        mask[max_pos] = False
        working = working[mask]
        working_indices = working_indices[mask]

    return latest_score, latest_flagged


class SeasonalHybridESD:
    """Seasonal-Hybrid ESD detector.

    Parameters (config ``params:``)
    -------------------------------
    ``period`` (int, default ``24``)
        Number of samples per seasonal cycle. For hourly buckets on a
        daily pattern, this is 24.
    ``max_anomalies`` (int, default ``10``)
        Maximum number of anomalies to search for in the ESD pass.
    ``alpha`` (float, default ``0.05``)
        Significance level for the ESD test.
    """

    name: ClassVar[str] = "SeasonalHybridESD"
    description: ClassVar[str] = (
        "Seasonal-Hybrid ESD: median-based seasonal decomposition plus "
        "generalised Extreme Studentized Deviate on the residual. For "
        "strongly seasonal signals with occasional outliers."
    )
    defaults: ClassVar[dict[str, Any]] = {
        "period": 24,
        "max_anomalies": 10,
        "alpha": 0.05,
    }
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="period",
            type_="int",
            default=24,
            doc="Samples per seasonal cycle (e.g. 24 for hourly on daily pattern).",
            minimum=2,
        ),
        ParamSpec(
            name="max_anomalies",
            type_="int",
            default=10,
            doc="Maximum number of anomalies the ESD pass searches for.",
            minimum=1,
        ),
        ParamSpec(
            name="alpha",
            type_="float",
            default=0.05,
            doc="Significance level for the ESD test (e.g. 0.05 = 5%).",
            minimum=0.001,
            maximum=0.5,
        ),
    )

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        del freq
        stats_mod = _try_import_scipy_stats()
        period = int(params.get("period", self.defaults["period"]))
        max_anomalies = int(params.get("max_anomalies", self.defaults["max_anomalies"]))
        alpha = float(params.get("alpha", self.defaults["alpha"]))

        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(df["timestamp"].iloc[-1])
        latest_y = float(values[-1])

        if values.size < period + 3:
            return pd.DataFrame(
                [
                    {
                        "timestamp": latest_ts,
                        "score": 0.0,
                        "baseline": latest_y,
                        "is_outside": False,
                    }
                ]
            )

        seasonal, residual = _median_seasonal_decompose(values, period)
        score, _ = _generalized_esd(residual, max_anomalies, alpha, stats_mod)

        # Baseline is the seasonal component at the latest point.
        baseline = float(seasonal[-1])

        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": score,
                    "baseline": baseline,
                    "is_outside": False,
                }
            ]
        )
