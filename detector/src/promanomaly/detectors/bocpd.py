"""Bayesian Online Change-Point Detection (BOCPD).

Adams & MacKay (2007) "Bayesian Online Changepoint Detection". Models the
observation stream with a Normal-Gamma conjugate prior (unknown mean and
variance) and tracks a posterior distribution over the *run length* — the
number of samples since the last change-point. The posterior probability
of run length zero at the latest step is the score we emit.

The score is a probability in ``[0, 1]``, not a sigma multiplier — so the
default firing threshold is ``0.5``, surfaced via the ``threshold``
parameter which the runner picks up as a per-detector override on
``alert_thresholds.score``.

Vendored implementation in plain numpy. The ``bocd`` library is the
reference; we keep the dependency footprint small (no scipy) because the
math fits in ~80 lines.
"""

from __future__ import annotations

import math
from typing import Any, ClassVar

import numpy as np
import pandas as pd

from .base import ParamSpec

try:
    from scipy.special import gammaln as _scipy_gammaln  # type: ignore[import-untyped]
except ImportError:
    _scipy_gammaln = None


def _vec_lgamma(values: np.ndarray[Any, Any]) -> np.ndarray[Any, Any]:
    """Vectorised ``math.lgamma`` with a scipy fast-path.

    BOCPD evaluates the Student's-t predictive density at every
    run-length on every sample, so the lgamma calls dominate detector
    compute. ``scipy.special.gammaln`` is 50-100x faster than the
    pure-Python loop and is already a transitive dependency (ruptures,
    pandas, statsmodels all pull it in). When it isn't installed —
    minimal-image deployments without the ``[analyze]`` extra — we
    fall back to a numpy ``fromiter`` over ``math.lgamma`` which
    matches the previous implementation.
    """
    if _scipy_gammaln is not None:
        return np.asarray(_scipy_gammaln(values), dtype=float)
    return np.fromiter((math.lgamma(float(v)) for v in values), dtype=float, count=int(values.size))


def _student_t_logpdf(
    x: float,
    df: np.ndarray[Any, Any],
    loc: np.ndarray[Any, Any],
    scale: np.ndarray[Any, Any],
) -> np.ndarray[Any, Any]:
    """Log-pdf of the (location-scale) Student's-t distribution.

    Vectorised over ``df``/``loc``/``scale`` so BOCPD can evaluate the
    predictive density at every run-length in one shot. Equivalent to
    ``scipy.stats.t.logpdf(x, df, loc=loc, scale=scale)`` but
    self-contained.
    """
    scale = np.maximum(scale, 1e-12)
    z = (x - loc) / scale
    half_df = df / 2.0
    half_df_plus_one = (df + 1.0) / 2.0
    log_gamma_num = _vec_lgamma(half_df_plus_one)
    log_gamma_den = _vec_lgamma(half_df)
    result: np.ndarray[Any, Any] = (
        log_gamma_num
        - log_gamma_den
        - 0.5 * np.log(df * math.pi)
        - np.log(scale)
        - 0.5 * (df + 1.0) * np.log(1.0 + z * z / df)
    )
    return result


def _logsumexp(values: np.ndarray[Any, Any]) -> float:
    """Numerically stable log-sum-exp; small enough to vendor."""
    if values.size == 0:
        return float("-inf")
    m = float(np.max(values))
    if not math.isfinite(m):
        return m
    return m + float(np.log(np.sum(np.exp(values - m))))


class BOCPD:
    """Bayesian Online Change-Point Detection (Normal-Gamma conjugate model).

    Parameters (config ``params:``)
    -------------------------------
    ``hazard`` (int, default ``250``)
        Expected interval between change-points in samples. The constant
        hazard rate used internally is ``1 / hazard``. The default of
        250 is the ``bocd`` library convention.
    ``threshold`` (float, default ``0.5``)
        Posterior change-point probability at which
        ``anomaly_outside_threshold`` (and the
        ``anomaly_change_point_total`` counter) fires. The runner reads
        this as a per-detector override of
        ``alert_thresholds.score`` because BOCPD's score is a probability,
        not a sigma multiplier.
    ``lag`` (int, default ``15``)
        How many recent samples count as "the current run". The emitted
        score is the posterior mass at ``r_t <= lag`` — i.e. the
        probability that the current run started within the last
        ``lag`` samples (inclusive). A flat probability at ``r_t = 0``
        is always equal to the prior hazard rate (see Adams & MacKay
        2007), so a small lag-window is the standard way to surface
        the signal. The default of 15 keeps BOCPD competitive with
        MAD/Hampel in the auto-select calibration cycle (which injects
        step changes at 90% of the window), without bloating the
        detection window past operationally useful values.

    Output columns
    --------------
    - ``score``: posterior probability that a change-point happened
      within the last ``lag`` samples, in ``[0, 1]``.
    - ``baseline``: estimated mean of the most-recent regime (the
      MAP-weighted posterior mean across run lengths).
    - ``is_outside``: ``True`` when ``score >= threshold``.
    - ``change_point``: ``True`` on the same condition; feeds
      ``anomaly_change_point_total``.
    """

    name: ClassVar[str] = "BOCPD"
    description: ClassVar[str] = (
        "Bayesian online change-point detection. Score is the posterior "
        "probability of a change-point at the latest sample (in [0, 1]). "
        "Default firing threshold 0.5 via per-detector override."
    )
    defaults: ClassVar[dict[str, Any]] = {"hazard": 250, "threshold": 0.5, "lag": 15}
    param_spec: ClassVar[tuple[ParamSpec, ...]] = (
        ParamSpec(
            name="hazard",
            type_="int",
            default=250,
            doc=(
                "Expected interval between change-points in samples. "
                "Internal hazard rate is 1/hazard; larger values make the "
                "detector more conservative."
            ),
        ),
        ParamSpec(
            name="threshold",
            type_="float",
            default=0.5,
            doc=(
                "Posterior probability >= threshold flags a change-point. "
                "Override of alert_thresholds.score, applied by the runner."
            ),
        ),
        ParamSpec(
            name="lag",
            type_="int",
            default=15,
            doc=(
                "Inclusive run-length window counted as 'recent'. The score "
                "is the posterior mass at r_t <= lag."
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
        hazard_raw = params.get("hazard", self.defaults["hazard"])
        try:
            hazard = int(hazard_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"BOCPD.hazard must be an integer, got {hazard_raw!r}") from exc
        if hazard < 1:
            raise ValueError(f"BOCPD.hazard must be >= 1, got {hazard}")
        threshold_raw = params.get("threshold", self.defaults["threshold"])
        try:
            threshold = float(threshold_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"BOCPD.threshold must be numeric, got {threshold_raw!r}") from exc
        lag_raw = params.get("lag", self.defaults["lag"])
        try:
            lag = max(1, int(lag_raw))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"BOCPD.lag must be an integer, got {lag_raw!r}") from exc

        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(df["timestamp"].iloc[-1])

        h = 1.0 / float(hazard)
        log_h = math.log(h)
        log_1mh = math.log(1.0 - h)

        # Prior: weak Normal-Gamma centred on the overall sample mean.
        # Setting beta0 to half the sample variance gives an informed but
        # not over-confident prior — the detector still adapts to any
        # regime that emerges.
        mu0 = float(np.mean(values))
        kappa0 = 1.0
        alpha0 = 1.0
        beta0 = max(float(np.var(values)) * 0.5, 1e-6)

        mu = np.array([mu0])
        kappa = np.array([kappa0])
        alpha = np.array([alpha0])
        beta = np.array([beta0])

        # Run-length posterior in log space (at the "start of step t").
        # Length grows with t; the array represents P(r_t = i | x_1:t-1).
        log_R = np.array([0.0])  # log(1)

        # Final-step values; overwritten in the loop.
        final_log_R = log_R
        final_mu = mu

        for x in values:
            df_t = 2.0 * alpha
            scale = np.sqrt(beta * (kappa + 1.0) / (alpha * kappa))
            log_pred = _student_t_logpdf(float(x), df_t, mu, scale)

            # Growth: stay at run length r_{t-1}+1 with prob (1 - H).
            log_growth = log_R + log_pred + log_1mh
            # Change-point: collapse to r_t = 0 with prob H.
            log_cp = _logsumexp(log_R + log_pred + log_h)

            new_log_R = np.empty(log_growth.size + 1)
            new_log_R[0] = log_cp
            new_log_R[1:] = log_growth
            new_log_R -= _logsumexp(new_log_R)  # normalise

            # Sufficient-statistic update for run-length r > 0.
            new_mu = (kappa * mu + x) / (kappa + 1.0)
            new_kappa = kappa + 1.0
            new_alpha = alpha + 0.5
            new_beta = beta + (kappa * (x - mu) ** 2) / (2.0 * (kappa + 1.0))

            mu = np.concatenate(([mu0], new_mu))
            kappa = np.concatenate(([kappa0], new_kappa))
            alpha = np.concatenate(([alpha0], new_alpha))
            beta = np.concatenate(([beta0], new_beta))
            log_R = new_log_R

            final_log_R = log_R
            final_mu = mu

        weights = np.exp(final_log_R)
        # The "change-point at the latest point" score is the posterior
        # mass at short run lengths (r_t <= lag, inclusive). P(r_t = 0)
        # on its own equals the prior hazard at every step, so we use
        # the cumulative mass at the recent run-lengths — the standard
        # way to surface the change-point signal from a BOCPD posterior.
        recent_lag = min(lag + 1, weights.size)
        # Clamp into [0, 1] — the weights *should* be a normalised
        # probability distribution, but the underlying floating-point
        # logsumexp + scipy.special.gammaln path occasionally returns
        # 1.0 + 1e-16 due to round-off. Clamping keeps the published
        # score within the documented contract without changing the
        # firing decision (the threshold is far from the edges).
        cp_prob = float(min(1.0, max(0.0, weights[:recent_lag].sum())))
        # MAP-style baseline: weighted average of regime means by their
        # posterior run-length mass. Falls back to the overall mean if
        # the posterior is degenerate (all zero mass).
        total = float(weights.sum())
        baseline = float(np.sum(weights * final_mu) / total) if total > 0.0 else mu0
        is_outside = cp_prob >= threshold

        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": cp_prob,
                    "baseline": baseline,
                    "is_outside": bool(is_outside),
                    "change_point": bool(is_outside),
                }
            ]
        )
