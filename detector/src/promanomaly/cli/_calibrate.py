"""Pure helpers for the ``calibrate-buckets`` subcommand.

The autocorrelation analysis and detector-recommendation logic live
here so they can be unit-tested without invoking Click. The CLI surface
itself stays in :mod:`cli.__init__`.
"""

from __future__ import annotations

from typing import Any

import click

from ..source import QuerySeries

_DAY = 86400.0
_WEEK = 7 * _DAY
_HOUR = 3600.0


def _analyse_bucket_structure(
    *,
    series_list: list[QuerySeries],
    step_seconds: float,
    window_seconds: float,
) -> dict[str, Any]:
    """Autocorrelation analysis: identify dominant periods + recommend a detector.

    Aggregates across every returned series — operators typically point
    this at a single PromQL that already filters to the metric they care
    about, but if the result has multiple series we want the cohort-wide
    pattern. The detector recommendation only fires when the dominant
    period sits near a known stratified-detector lag (24h, 7d). Anything
    else returns ``None`` for the recommendation so we don't pretend a
    random ACF peak is a real periodicity.
    """
    import numpy as np

    # Stack series into a 2D array padded with NaNs, then take the
    # nan-mean per timestamp so series that don't share every sample
    # don't break the calculation.
    columns: list[np.ndarray[Any, Any]] = []
    for series in series_list:
        values = series.samples["y"].to_numpy(dtype=float, copy=False)
        if values.size:
            columns.append(values)
    if not columns:
        return {"status": "empty", "recommended_detector": None}
    max_len = max(c.size for c in columns)
    stacked = np.full((len(columns), max_len), np.nan, dtype=float)
    for idx, col in enumerate(columns):
        stacked[idx, : col.size] = col
    aggregated = np.nanmean(stacked, axis=0)
    aggregated = aggregated[~np.isnan(aggregated)]
    if aggregated.size < 8:
        return {
            "status": "too_few_samples",
            "samples": int(aggregated.size),
            "recommended_detector": None,
        }

    centred = aggregated - float(np.mean(aggregated))
    # Lag-0 autocovariance (== variance * N). The ACF at lag k is the
    # ratio autocovariance(k) / autocovariance(0), so dividing each
    # lag's dot product by this value produces correlations in [-1, 1].
    c0 = float(np.dot(centred, centred))
    # Float noise on a strictly-constant input still produces a tiny
    # positive c0; treat anything below numerical noise as constant so
    # the recommendation surface doesn't claim periodicity on a flat
    # signal.
    if c0 <= 1e-9:
        return {
            "status": "constant_signal",
            "samples": int(aggregated.size),
            "recommended_detector": None,
        }
    # Cap the lag scan to half the sample count — beyond that the ACF
    # estimator gets noisy as the overlap shrinks. We also stop short
    # of one week if the window doesn't cover it.
    max_lag = min(aggregated.size // 2, int(8 * 86400.0 / step_seconds))
    acf = np.zeros(max_lag, dtype=float)
    acf[0] = 1.0  # ACF at lag 0 is always 1.0 by definition
    for lag in range(1, max_lag):
        acf[lag] = float(np.dot(centred[lag:], centred[:-lag])) / c0

    # Find local maxima in the ACF — those are candidate periods. Skip
    # the very-short-lag noise (< step minutes) which is autocorrelation
    # from smoothing, not real periodicity.
    candidates: list[tuple[int, float]] = []
    min_lag = max(2, int(900.0 / step_seconds))  # at least 15 minutes
    for lag in range(min_lag, max_lag - 1):
        if acf[lag] > acf[lag - 1] and acf[lag] > acf[lag + 1] and acf[lag] > 0.1:
            candidates.append((lag, acf[lag]))
    candidates.sort(key=lambda r: r[1], reverse=True)
    top_candidates = candidates[:5]

    dominant_period_seconds: float | None = None
    dominant_strength = 0.0
    if top_candidates:
        dominant_period_seconds = top_candidates[0][0] * step_seconds
        dominant_strength = top_candidates[0][1]

    recommendation = _recommend_detector(
        dominant_period_seconds=dominant_period_seconds,
        candidates=[(lag * step_seconds, score) for lag, score in top_candidates],
        window_seconds=window_seconds,
    )

    return {
        "status": "ok",
        "samples": int(aggregated.size),
        "step_seconds": float(step_seconds),
        "window_seconds": float(window_seconds),
        "dominant_period_seconds": dominant_period_seconds,
        "dominant_strength": dominant_strength,
        "top_candidates": [
            {
                "period_seconds": lag * step_seconds,
                "period_label": _humanise_seconds(lag * step_seconds),
                "strength": float(score),
            }
            for lag, score in top_candidates
        ],
        "recommended_detector": recommendation,
    }


def _recommend_detector(
    *,
    dominant_period_seconds: float | None,
    candidates: list[tuple[float, float]],
    window_seconds: float,
) -> dict[str, Any] | None:
    """Map the dominant ACF period onto a stratified detector config."""
    if dominant_period_seconds is None:
        return None
    weekly_present = any(_close_to(period, _WEEK, tol=0.1) for period, _ in candidates)
    daily_present = any(_close_to(period, _DAY, tol=0.1) for period, _ in candidates)
    has_weekly_window = window_seconds >= 2 * _WEEK

    if weekly_present and has_weekly_window:
        return {
            "name": "DayOfWeekMAD",
            "reason": (
                "Weekly periodicity detected; (day-of-week, hour) "
                "stratification matches the signal shape."
            ),
            "params": {"lookback": "4w", "bucket_width": "1h"},
            "config_snippet": (
                "detectors:\n"
                "  - name: DayOfWeekMAD\n"
                "    params:\n"
                "      lookback: 4w\n"
                "      bucket_width: 1h\n"
            ),
        }
    if daily_present:
        return {
            "name": "HourOfDayMAD",
            "reason": (
                "Daily periodicity detected; hour-of-day stratification "
                "captures the dominant structure."
            ),
            "params": {"lookback": "7d", "bucket_width": "1h"},
            "config_snippet": (
                "detectors:\n"
                "  - name: HourOfDayMAD\n"
                "    params:\n"
                "      lookback: 7d\n"
                "      bucket_width: 1h\n"
            ),
        }
    # Sub-hour periodicity → fine-grained bucket width with the
    # short-lookback hour-of-day detector; user gets a starting point
    # even when the signal doesn't fit the canonical 24h/7d shapes.
    if dominant_period_seconds < _HOUR and dominant_period_seconds > 0:
        bucket = _round_to_dividing_bucket(dominant_period_seconds)
        return {
            "name": "HourOfDayMAD",
            "reason": (
                f"Sub-hour periodicity detected ({_humanise_seconds(dominant_period_seconds)}); "
                "narrow the bucket_width to match."
            ),
            "params": {"lookback": "7d", "bucket_width": bucket},
            "config_snippet": (
                "detectors:\n"
                "  - name: HourOfDayMAD\n"
                "    params:\n"
                "      lookback: 7d\n"
                f"      bucket_width: {bucket}\n"
            ),
        }
    return None


def _close_to(value: float, target: float, *, tol: float) -> bool:
    """True when ``value`` is within ``tol`` (relative) of ``target``."""
    if target == 0.0:
        return value == 0.0
    return abs(value - target) / target <= tol


def _round_to_dividing_bucket(period_seconds: float) -> str:
    """Pick a bucket_width that divides 24h and is close to ``period_seconds``."""
    # Sorted ascending so we always pick the narrower bucket on a tie.
    candidates_s = [60, 300, 600, 900, 1800, 3600]
    best = min(candidates_s, key=lambda c: abs(c - period_seconds))
    if best < 3600 and best % 60 == 0:
        minutes = best // 60
        return f"{minutes}m"
    return "1h"


def _humanise_seconds(seconds: float) -> str:
    if seconds >= _WEEK:
        return f"{seconds / _WEEK:.1f}w"
    if seconds >= _DAY:
        return f"{seconds / _DAY:.1f}d"
    if seconds >= _HOUR:
        return f"{seconds / _HOUR:.1f}h"
    if seconds >= 60.0:
        return f"{seconds / 60.0:.1f}m"
    return f"{seconds:.1f}s"


def _print_calibrate_text(analysis: dict[str, Any]) -> None:
    status = analysis.get("status")
    if status != "ok":
        click.echo(f"calibration failed: {status}")
        return
    click.echo(f"samples analysed       : {analysis['samples']}")
    click.echo(f"step                   : {_humanise_seconds(analysis['step_seconds'])}")
    click.echo(f"window                 : {_humanise_seconds(analysis['window_seconds'])}")
    dominant = analysis.get("dominant_period_seconds")
    if dominant:
        click.echo(
            f"dominant period        : {_humanise_seconds(dominant)} "
            f"(strength {analysis['dominant_strength']:.3f})"
        )
    candidates = analysis.get("top_candidates") or []
    if candidates:
        click.echo("top candidate periods  :")
        for cand in candidates:
            click.echo(f"  - {cand['period_label']:>8}   strength {cand['strength']:.3f}")
    rec = analysis.get("recommended_detector")
    if rec is None:
        click.echo("recommended detector   : (no stratified detector clearly fits)")
        click.echo(
            "  Consider MAD / Hampel on the rolling window, or rerun with a longer --from window."
        )
        return
    click.echo(f"recommended detector   : {rec['name']}")
    click.echo(f"reason                 : {rec['reason']}")
    click.echo("")
    click.echo("Paste into your group's query:")
    click.echo("")
    click.echo(rec["config_snippet"].rstrip())
