"""Stateless Isolation-Forest anomaly detector.

Refits an Isolation Forest on the rolling window every run — **nothing
persisted across runs or restarts**, which keeps it inside
promanomaly's stateless promise rather than in the deferred
supervised-ML class.

scikit-learn is imported lazily so the module loads cleanly in
environments where sklearn isn't installed. Install with:
``pip install promanomaly-iforest`` (which pulls scikit-learn).
"""

from __future__ import annotations

from typing import Any, ClassVar

import numpy as np
import pandas as pd
from promanomaly.detectors.base import ParamSpec


def _try_import_isolation_forest() -> Any:
    """Resolve sklearn IsolationForest at call time."""
    try:
        from sklearn.ensemble import IsolationForest as SklearnIF
    except ImportError as exc:
        raise RuntimeError(
            "IsolationForest requires scikit-learn — install with: "
            "pip install promanomaly-iforest (or pip install scikit-learn)"
        ) from exc
    return SklearnIF


class IsolationForest:
    """Stateless Isolation-Forest detector.

    Refits on the rolling window every run. The latest point's
    normalised anomaly score becomes ``anomaly_score``.

    Parameters (config ``params:``)
    -------------------------------
    ``n_estimators`` (int, default ``100``)
        Number of trees in the forest.
    ``contamination`` (float, default ``0.05``)
        Expected proportion of outliers in the window. Affects the
        decision threshold used internally by sklearn; the raw score
        is re-normalised to the shared score scale regardless.
    """

    name: ClassVar[str] = "IsolationForest"
    description: ClassVar[str] = (
        "Stateless Isolation Forest: refits on the rolling window "
        "every run, nothing persisted across restarts. Unsupervised "
        "detector with a different inductive bias from statistical "
        "baselines."
    )
    defaults: ClassVar[dict[str, Any]] = {
        "n_estimators": 100,
        "contamination": 0.05,
    }
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="n_estimators",
            type_="int",
            default=100,
            doc="Number of trees in the isolation forest.",
            minimum=10,
        ),
        ParamSpec(
            name="contamination",
            type_="float",
            default=0.05,
            doc=(
                "Expected outlier proportion in the window. Affects sklearn's "
                "internal threshold; the raw score is re-normalised regardless."
            ),
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
        sklearn_if = _try_import_isolation_forest()
        n_estimators = int(params.get("n_estimators", self.defaults["n_estimators"]))
        contamination = float(params.get("contamination", self.defaults["contamination"]))

        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(df["timestamp"].iloc[-1])
        latest_y = float(values[-1])

        # Need enough data to fit.
        if values.size < 10:
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

        # Reshape for sklearn (n_samples, 1).
        x_all = values.reshape(-1, 1)

        model = sklearn_if(
            n_estimators=n_estimators,
            contamination=contamination,
            random_state=42,
        )
        model.fit(x_all)

        # score_samples returns values where more negative = more anomalous.
        # The offset is chosen so that regular samples score near 0 and
        # anomalies score well above the default threshold of 3.0.
        raw_scores = model.score_samples(x_all)
        latest_raw = float(raw_scores[-1])

        # Normalise: compute MAD of the raw scores, then express the
        # latest sample's deviation from the median raw score in MAD
        # units. Flip sign so more-anomalous = higher score.
        median_raw = float(np.median(raw_scores))
        mad_raw = float(np.median(np.abs(raw_scores - median_raw)))
        deviation = median_raw - latest_raw  # positive when anomalous
        if mad_raw > 0.0:
            score = deviation / mad_raw
        elif deviation > 0.0:
            # Degenerate case: most raw scores are identical.
            # Fall back to std as the denominator.
            std_raw = float(np.std(raw_scores))
            score = deviation / std_raw if std_raw > 0.0 else 0.0
        else:
            score = 0.0

        # Clamp to non-negative.
        score = max(score, 0.0)

        baseline = float(np.median(values))

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
