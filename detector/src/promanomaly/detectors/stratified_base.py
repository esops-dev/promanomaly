"""Shared scaffolding for stratified-bucket MAD detectors.

Both :class:`HourOfDayMAD` and :class:`DayOfWeekMAD` score the latest
sample against historical observations of the same bucket (hour of day,
or day-of-week x hour). What differs between them is the bucket grid
shape; the parameter parsing, query-window-extension logic, refresh
cadence, fit-score skeleton, and sparse-bucket fallback are identical.

This base captures all the shared scaffolding and leaves subclasses to
supply only the bucket-coordinate computation.

Stratified does NOT mean forecasting. The detectors score the present
against history of the same bucket; they never predict future values.
"""

from __future__ import annotations

import math
from typing import Any, ClassVar

import numpy as np
import pandas as pd

from ..config import parse_duration
from .base import ParamSpec


class StratifiedMADBase:
    """Mixin/abstract base for stratified MAD detectors.

    Subclasses must set the standard detector class attributes
    (``name``, ``description``, ``defaults``, ``param_spec``) and
    implement two hooks:

    - :meth:`_history_bucket_codes` — vectorised bucket assignment
      across an array of historical timestamps. Must return a 1D
      ``int64`` array of the same length, where two indices share a
      value iff their timestamps fall in the same bucket.

    - :meth:`_latest_bucket_code` — scalar bucket code for the latest
      timestamp, comparable against entries returned by the vectorised
      helper.

    Subclasses are free to encode multi-dimensional buckets (e.g.
    ``day_of_week x hour_of_day``) into a single int code however they
    like; this base does not interpret the codes.
    """

    name: ClassVar[str] = ""
    description: ClassVar[str] = ""
    defaults: ClassVar[dict[str, Any]] = {}
    param_spec: ClassVar[tuple[ParamSpec, ...]] = ()

    # ------------------------------------------------------------------
    # Window contracts — surfaced to the runner so it knows to pull a
    # longer history for this query than the group default allows for.
    # ------------------------------------------------------------------
    @classmethod
    def query_window_seconds(cls, params: dict[str, Any]) -> float | None:
        """Inform the runner this detector needs a longer query window."""
        lookback = params.get("lookback", cls.defaults.get("lookback", ""))
        try:
            return float(parse_duration(lookback))
        except (TypeError, ValueError):
            return None

    @classmethod
    def baseline_refresh_interval_seconds(cls, params: dict[str, Any]) -> float | None:
        """Cadence at which the long baseline fetch may be reused.

        Returning a value opts the detector into the runner's
        sliding-window strategy: the heavy long-history query runs once
        per refresh interval, and every detector run still pulls the
        latest short window and stitches it onto the cached baseline
        before scoring.
        """
        raw = params.get(
            "baseline_refresh_interval",
            cls.defaults.get("baseline_refresh_interval", ""),
        )
        try:
            return float(parse_duration(raw))
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------
    def _history_bucket_codes(
        self, history_ts: np.ndarray[Any, Any], bucket_seconds: float
    ) -> np.ndarray[Any, Any]:
        raise NotImplementedError

    def _latest_bucket_code(self, latest_ts: float, bucket_seconds: float) -> int:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Shared scoring pipeline
    # ------------------------------------------------------------------
    @classmethod
    def _parse_bucket_seconds(cls, params: dict[str, Any]) -> float:
        bucket_raw = params.get("bucket_width", cls.defaults["bucket_width"])
        try:
            bucket_seconds = float(parse_duration(bucket_raw))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{cls.name}.bucket_width must be a duration, got {bucket_raw!r}"
            ) from exc
        if bucket_seconds <= 0:
            raise ValueError(f"{cls.name}.bucket_width must be positive, got {bucket_seconds}")
        # Buckets only make sense as a divisor of a 24h day; reject
        # non-divisors so the bucket grid is unambiguous.
        if not math.isclose(86400.0 % bucket_seconds, 0.0, abs_tol=1e-6) and not math.isclose(
            86400.0 % bucket_seconds, bucket_seconds, abs_tol=1e-6
        ):
            raise ValueError(f"{cls.name}.bucket_width ({bucket_raw}) must divide 24h evenly")
        return bucket_seconds

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        del freq
        bucket_seconds = self._parse_bucket_seconds(params)

        timestamps = df["timestamp"].to_numpy(dtype=float, copy=False)
        values = df["y"].to_numpy(dtype=float, copy=False)
        latest_ts = float(timestamps[-1])
        latest_y = float(values[-1])
        latest_code = self._latest_bucket_code(latest_ts, bucket_seconds)

        history_ts = timestamps[:-1]
        history_values = values[:-1]
        if history_ts.size == 0:
            return _zero_score_row(latest_ts, float(np.median(values)))

        history_codes = self._history_bucket_codes(history_ts, bucket_seconds)
        same_bucket_values = history_values[history_codes == latest_code]

        if same_bucket_values.size == 0:
            # Sparse-bucket fallback: not enough history for this cell
            # yet. The warmup_policy and confidence gauge are the right
            # places to surface "we don't have enough data"; the score
            # stays silent rather than fabricating one.
            return _zero_score_row(latest_ts, float(np.median(values)))

        bucket_median = float(np.median(same_bucket_values))
        bucket_mad = float(np.median(np.abs(same_bucket_values - bucket_median)))
        score = 0.0 if bucket_mad == 0.0 else abs(latest_y - bucket_median) / bucket_mad

        return pd.DataFrame(
            [
                {
                    "timestamp": latest_ts,
                    "score": score,
                    "baseline": bucket_median,
                    "is_outside": False,
                }
            ]
        )


def _zero_score_row(latest_ts: float, baseline: float) -> pd.DataFrame:
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
