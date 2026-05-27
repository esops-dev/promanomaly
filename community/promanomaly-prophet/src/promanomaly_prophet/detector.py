"""Prophet-residual anomaly detector.

Fits a Prophet baseline on the rolling window and scores the latest
sample by the magnitude of its residual normalised by Prophet's
uncertainty band half-width. Score units are roughly comparable to
MAD/Hampel — number of band-widths from the predicted point — so the
group-level ``alert_thresholds.score`` knob (default 3.0) governs the
``anomaly_outside_threshold`` bit without surprise.

Prophet itself is imported lazily inside ``fit_score`` so the module
imports cleanly in environments where Prophet isn't installed
(notably the host promanomaly's CI, which doesn't carry the heavy
cmdstan dependency). When that happens the detector raises a clean
:class:`RuntimeError` with the install hint, rather than letting the
ImportError surface from inside the runner's thread pool.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pandas as pd

# The host detector ships ``ParamSpec`` as part of its public detector
# contract. Importing it is part of the third-party plug-in contract;
# the import surfaces the dependency explicitly so a stale promanomaly
# install fails at module-load rather than at metadata-introspection.
from promanomaly.detectors.base import ParamSpec


def _try_import_prophet() -> Any:
    """Resolve Prophet at call time so module import stays cheap.

    Returns the ``Prophet`` class on success, raises ``RuntimeError``
    with an actionable install hint on failure. We catch the
    ``ImportError`` here rather than letting it bubble out of the
    runner because the runner translates any exception into
    ``anomaly_failures_total{reason=detector_exception}`` and the
    operator never sees the real cause without trawling logs.
    """
    try:
        from prophet import Prophet
    except ImportError as exc:
        raise RuntimeError(
            "ProphetResidual requires the 'prophet' package — install with: "
            "pip install promanomaly-prophet[full] (or pip install prophet)"
        ) from exc
    return Prophet


def _build_history(df: pd.DataFrame) -> pd.DataFrame:
    """Translate promanomaly's ``timestamp,y`` frame into Prophet's ``ds,y`` shape.

    Prophet expects unix-seconds in a pandas Timestamp column named
    ``ds``; the host detector hands us float seconds in ``timestamp``.
    """
    return pd.DataFrame(
        {
            "ds": pd.to_datetime(df["timestamp"].to_numpy(), unit="s", utc=True),
            "y": df["y"].to_numpy(dtype=float, copy=False),
        }
    )


class ProphetResidual:
    """Anomaly detector that scores the latest sample against a Prophet baseline.

    Parameters (config ``params:``)
    -------------------------------
    ``changepoint_prior_scale`` (float, default ``0.05``)
        Forwarded to Prophet — controls how easily the trend bends at
        change-points. Lower = smoother baseline (more residual mass);
        higher = baseline tracks recent shifts (less residual mass).
    ``interval_width`` (float, default ``0.95``)
        Forwarded to Prophet — the predictive-interval width used as
        the score denominator. Wider intervals make the detector more
        permissive at the score level.
    ``seasonality_mode`` (str, default ``"additive"``)
        ``additive`` or ``multiplicative``; forwarded to Prophet.
    """

    name: ClassVar[str] = "ProphetResidual"
    description: ClassVar[str] = (
        "Fits a Prophet baseline on the rolling window and scores the latest "
        "sample by the residual normalised by Prophet's predictive-interval "
        "half-width. Best on signals with learnable seasonality."
    )
    defaults: ClassVar[dict[str, Any]] = {
        "changepoint_prior_scale": 0.05,
        "interval_width": 0.95,
        "seasonality_mode": "additive",
    }
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="changepoint_prior_scale",
            type_="float",
            default=0.05,
            doc="Prophet trend-flexibility prior; lower = smoother baseline.",
        ),
        ParamSpec(
            name="interval_width",
            type_="float",
            default=0.95,
            doc="Width of the predictive interval used as the score denominator.",
        ),
        ParamSpec(
            name="seasonality_mode",
            type_="str",
            default="additive",
            doc="Prophet seasonality mode: 'additive' or 'multiplicative'.",
        ),
    )

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        del freq
        prophet_cls = _try_import_prophet()
        cps = float(params.get("changepoint_prior_scale", self.defaults["changepoint_prior_scale"]))
        iw = float(params.get("interval_width", self.defaults["interval_width"]))
        mode = str(params.get("seasonality_mode", self.defaults["seasonality_mode"]))

        history = _build_history(df)
        model = prophet_cls(
            changepoint_prior_scale=cps,
            interval_width=iw,
            seasonality_mode=mode,
        )
        model.fit(history)
        # Forecast just the latest sample's timestamp so we don't pay
        # for extrapolation we'd discard.
        future = history.iloc[[-1]][["ds"]].copy()
        forecast = model.predict(future)
        row = forecast.iloc[-1]
        yhat = float(row.get("yhat", 0.0))
        yhat_lower = float(row.get("yhat_lower", yhat))
        yhat_upper = float(row.get("yhat_upper", yhat))

        latest_ts = float(df["timestamp"].iloc[-1])
        latest_y = float(df["y"].iloc[-1])

        # Half-band as the score denominator; small floor prevents
        # divide-by-zero on a perfectly-fit signal (all residuals near
        # zero, band width collapses).
        half_band = max((yhat_upper - yhat_lower) / 2.0, 1e-9)
        residual = abs(latest_y - yhat)
        score = residual / half_band

        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": score,
                    "baseline": yhat,
                    "baseline_lower": yhat_lower,
                    "baseline_upper": yhat_upper,
                    "is_outside": False,
                }
            ]
        )
