"""STL-residual detector.

Runs STL (Seasonal and Trend decomposition using Loess) over the
rolling window and scores the latest residual with MAD. A lighter-
weight option than Seasonal-Hybrid ESD for seasonal signals: robust
residual scoring without the ESD machinery.

Requires ``statsmodels`` at runtime — imported lazily so the module
loads cleanly even when statsmodels isn't installed. Install with:
``pip install promanomaly[seasonal]`` (or ``pip install statsmodels``).

This is still baseline detection, not forecasting: it decomposes the
present window, it does not predict future values.
"""

from __future__ import annotations

from typing import Any, ClassVar

import numpy as np
import pandas as pd

from .base import ParamSpec


def _try_import_stl() -> Any:
    """Resolve statsmodels STL at call time."""
    try:
        from statsmodels.tsa.seasonal import STL
    except ImportError as exc:
        raise RuntimeError(
            "STLResidualMAD requires statsmodels — install with: "
            "pip install promanomaly[seasonal] (or pip install statsmodels)"
        ) from exc
    return STL


class STLResidualMAD:
    """STL decomposition + MAD on residual.

    Parameters (config ``params:``)
    -------------------------------
    ``period`` (int, default ``24``)
        Number of samples per seasonal cycle.
    ``robust`` (bool, default ``True``)
        When True, use robust fitting in the STL decomposition so
        outliers in the window don't contaminate the trend/seasonal
        estimates.
    """

    name: ClassVar[str] = "STLResidualMAD"
    description: ClassVar[str] = (
        "STL decomposition over the rolling window, then MAD on the "
        "residual. Lighter than Seasonal-Hybrid ESD; emits trend + "
        "seasonal as baseline for dashboard overlay."
    )
    defaults: ClassVar[dict[str, Any]] = {
        "period": 24,
        "robust": True,
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
            name="robust",
            type_="bool",
            default=True,
            doc="Use robust fitting so outliers don't contaminate the decomposition.",
        ),
    )

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        del freq
        stl_cls = _try_import_stl()
        period = int(params.get("period", self.defaults["period"]))
        robust = bool(params.get("robust", self.defaults["robust"]))

        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(df["timestamp"].iloc[-1])

        # STL requires at least 2 full periods of data.
        min_size = 2 * period + 1
        if values.size < min_size:
            return pd.DataFrame(
                [
                    {
                        "timestamp": latest_ts,
                        "score": 0.0,
                        "baseline": float(values[-1]),
                        "is_outside": False,
                    }
                ]
            )

        # Fill NaNs with forward-fill then backward-fill for STL.
        series = pd.Series(values)
        series = series.ffill().bfill()
        clean = series.to_numpy(dtype=float)

        stl = stl_cls(clean, period=period, robust=robust)
        result = stl.fit()

        residual = result.resid
        trend = result.trend
        seasonal = result.seasonal

        # Score the latest residual with MAD.
        finite_resid = residual[np.isfinite(residual)]
        if finite_resid.size < 2:
            score = 0.0
        else:
            median_resid = float(np.median(finite_resid))
            mad = float(np.median(np.abs(finite_resid - median_resid)))
            latest_resid = float(residual[-1])
            if mad > 0.0:
                score = abs(latest_resid - median_resid) / mad
            elif abs(latest_resid - median_resid) > 0.0:
                # Degenerate case: all residuals are identical except
                # the latest.  Fall back to std as the denominator.
                std = float(np.std(finite_resid))
                score = abs(latest_resid - median_resid) / std if std > 0.0 else 0.0
            else:
                score = 0.0

        # Baseline = trend + seasonal at the latest point.
        baseline = float(trend[-1]) + float(seasonal[-1])

        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": score,
                    "baseline": baseline,
                    "baseline_lower": baseline - 3.0 * float(np.median(np.abs(finite_resid))),
                    "baseline_upper": baseline + 3.0 * float(np.median(np.abs(finite_resid))),
                    "is_outside": False,
                }
            ]
        )
