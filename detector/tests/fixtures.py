"""Synthetic time-series fixtures used across detector tests.

Covers the canonical shapes listed in the project guide (clean baseline, point
spike, clustered spikes, step change, gradual drift, variance shift, ...).
Each helper returns a DataFrame with ``timestamp`` / ``y`` columns so the
detectors can consume them unmodified.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _timestamps(n: int, step: float = 15.0, start: float = 0.0) -> np.ndarray:
    return start + np.arange(n) * step


def _frame(values: np.ndarray, step: float = 15.0) -> pd.DataFrame:
    return pd.DataFrame({"timestamp": _timestamps(values.size, step=step), "y": values})


def clean_baseline(
    n: int = 120, mean: float = 10.0, noise: float = 0.1, seed: int = 0
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    values = mean + rng.normal(0.0, noise, size=n)
    return _frame(values)


def point_spike(
    n: int = 120,
    mean: float = 10.0,
    noise: float = 0.1,
    spike: float = 10.0,
    seed: int = 0,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    values = mean + rng.normal(0.0, noise, size=n)
    values[-1] = mean + spike
    return _frame(values)


def clustered_spikes(
    n: int = 120,
    mean: float = 10.0,
    noise: float = 0.1,
    spike: float = 10.0,
    cluster: int = 5,
    seed: int = 0,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    values = mean + rng.normal(0.0, noise, size=n)
    values[-cluster:] = mean + spike
    return _frame(values)


def step_change(
    n: int = 120,
    mean: float = 10.0,
    step: float = 5.0,
    noise: float = 0.1,
    change_at: int | None = None,
    seed: int = 0,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    if change_at is None:
        change_at = n // 2
    values = mean + rng.normal(0.0, noise, size=n)
    values[change_at:] += step
    return _frame(values)


def gradual_drift(
    n: int = 120,
    mean: float = 10.0,
    slope: float = 0.05,
    noise: float = 0.1,
    seed: int = 0,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    values = mean + slope * np.arange(n) + rng.normal(0.0, noise, size=n)
    return _frame(values)


def variance_shift(
    n: int = 120,
    mean: float = 10.0,
    noise_low: float = 0.1,
    noise_high: float = 1.0,
    change_at: int | None = None,
    seed: int = 0,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    if change_at is None:
        change_at = n // 2
    values = mean + rng.normal(0.0, noise_low, size=n)
    values[change_at:] = mean + rng.normal(0.0, noise_high, size=n - change_at)
    return _frame(values)


def periodic_spike(
    n: int = 240,
    mean: float = 10.0,
    noise: float = 0.1,
    spike: float = 10.0,
    period: int = 60,
    seed: int = 0,
) -> pd.DataFrame:
    """Recurring anomaly at a regular cadence."""
    rng = np.random.default_rng(seed)
    values = mean + rng.normal(0.0, noise, size=n)
    for i in range(period - 1, n, period):
        values[i] = mean + spike
    return _frame(values)


def diurnal(
    n: int = 240,
    mean: float = 10.0,
    amplitude: float = 5.0,
    noise: float = 0.2,
    period: int = 24,
    anomaly_phase: int | None = None,
    anomaly_magnitude: float = 10.0,
    seed: int = 0,
) -> pd.DataFrame:
    """Clean periodic signal with optional anomaly at one phase."""
    rng = np.random.default_rng(seed)
    t = np.arange(n)
    values = mean + amplitude * np.sin(2 * np.pi * t / period) + rng.normal(0.0, noise, size=n)
    if anomaly_phase is not None:
        values[anomaly_phase] += anomaly_magnitude
    return _frame(values)


def gap_window(
    n: int = 120,
    mean: float = 10.0,
    noise: float = 0.1,
    gap_start: int = 50,
    gap_end: int = 70,
    seed: int = 0,
) -> pd.DataFrame:
    """A window of missing data (NaN) inside the lookback."""
    rng = np.random.default_rng(seed)
    values = mean + rng.normal(0.0, noise, size=n)
    values[gap_start:gap_end] = np.nan
    return _frame(values)


def short_window(n: int = 5, mean: float = 10.0) -> pd.DataFrame:
    return _frame(np.full(n, mean))
