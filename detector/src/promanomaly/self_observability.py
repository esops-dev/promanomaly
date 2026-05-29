"""Self-observability metrics for the detector itself.

The detector emits *correct* anomaly scores; this module gives operators
telemetry about the detector's own health so a silently degrading
component is caught before it stops catching anomalies. Four families,
all bounded by configuration size (group / detector) — none touch the
per-series cardinality budget:

* ``anomaly_detect_success_ratio{group, detector, window}`` — the
  fraction of attempted detector computations that completed without
  raising (timeout / exception), over a rolling window. Two windows are
  emitted (``1h`` and ``24h`` by default): the 1h flavour spots a
  detector that just started failing, the 24h flavour is the slower
  trend. A degrading detector surfaces here before its missed scores
  pollute downstream alerts.
The remaining three families are owned by the exporter / runner
directly because their natural representation is a plain
``prometheus_client`` Counter / Gauge:

* ``anomaly_group_cpu_seconds_total{group}`` — cumulative detector
  compute time attributed to a group (the sum of the per-detector
  wall-clock the runner already tracks in ``_DETECT_DURATIONS``); the
  runner ``inc()``s the counter at the end of each run.
* ``anomaly_group_memory_bytes{group}`` — a best-effort
  ``sys.getsizeof`` snapshot of the per-group output samples; the runner
  sets the gauge after each successful run.
* ``anomaly_series_staleness_seconds{group}`` and
  ``anomaly_snapshot_age_seconds{group}`` — render-time freshness gauges
  computed by the exporter directly from the snapshot store timestamps
  (see :mod:`promanomaly.exporter`).

Mirrors promforecast's ``self_observability`` module so the two tools
report their health the same way (``forecast_model_fit_success_ratio``
is the sibling of ``anomaly_detect_success_ratio``).
"""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass

# Default rolling windows for the success-ratio gauge. The 1h window
# spots a detector that just started failing; the 24h window is the
# slower trend. Both work against the chart's alert language
# ("ratio < 0.9 for 5m").
DEFAULT_SUCCESS_WINDOWS_SECONDS: tuple[int, ...] = (3600, 86400)


@dataclass(frozen=True)
class DetectOutcomeSample:
    """One aggregated (attempts, successes, timestamp) observation.

    One sample is recorded per group-run per detector — aggregating the
    per-series attempts inside that run into a single ``(attempts,
    successes)`` pair. This bounds the deque at *runs-per-window* rather
    than *series x runs-per-window*: a 10k-series group at a 1m refresh
    over 24h keeps 1440 samples per detector, not 14.4M.
    """

    attempts: int
    successes: int
    timestamp: float


@dataclass(frozen=True)
class SuccessRatioPoint:
    """Rendered point for ``anomaly_detect_success_ratio``."""

    group: str
    detector: str
    window_label: str
    ratio: float


class DetectOutcomeTracker:
    """Per-(group, detector) rolling success/attempt ledger.

    A ``deque`` per (group, detector) keeps every aggregated observation
    inside the longest configured window; :meth:`render` returns the
    fraction of successful attempts that fall inside each asked-for
    window. Observations older than the longest window are pruned at
    record time so the deque stays bounded at the operational
    cardinality the runner already imposes.
    """

    def __init__(self, *, windows_seconds: Iterable[int] = DEFAULT_SUCCESS_WINDOWS_SECONDS) -> None:
        self._windows_seconds: tuple[int, ...] = tuple(sorted({int(w) for w in windows_seconds}))
        if not self._windows_seconds:
            raise ValueError("DetectOutcomeTracker requires at least one window")
        self._max_window = max(self._windows_seconds)
        self._lock = threading.Lock()
        self._samples: dict[tuple[str, str], deque[DetectOutcomeSample]] = {}

    @property
    def windows_seconds(self) -> tuple[int, ...]:
        return self._windows_seconds

    def record(
        self,
        *,
        group: str,
        detector: str,
        attempts: int,
        successes: int,
        now: float | None = None,
    ) -> None:
        """Record one group-run's aggregated outcome for ``detector``.

        ``now`` defaults to ``time.time()`` and is overridable in tests
        so rolling-window expiry is deterministic without sleeps. A run
        in which the detector was never attempted (``attempts == 0``) is
        ignored so an idle detector doesn't dilute the ratio.
        """
        if attempts <= 0:
            return
        ts = time.time() if now is None else now
        with self._lock:
            bucket = self._samples.setdefault((group, detector), deque())
            bucket.append(DetectOutcomeSample(attempts=attempts, successes=successes, timestamp=ts))
            self._prune_locked(bucket, ts)

    def render(self, *, now: float | None = None) -> list[SuccessRatioPoint]:
        """Snapshot the success ratio at every configured window.

        Returns one point per (group, detector, window) that has at
        least one attempt inside the window. Empty windows are skipped —
        a 0/0 ratio is not a "100% healthy" signal, and emitting NaN
        would just confuse the alert path.
        """
        ts = time.time() if now is None else now
        out: list[SuccessRatioPoint] = []
        with self._lock:
            for (group, detector), bucket in self._samples.items():
                self._prune_locked(bucket, ts)
                for window in self._windows_seconds:
                    ratio = _ratio_in_window(bucket, ts, window)
                    if ratio is None:
                        continue
                    out.append(
                        SuccessRatioPoint(
                            group=group,
                            detector=detector,
                            window_label=_format_window(window),
                            ratio=ratio,
                        )
                    )
        return out

    def retain_groups(self, names: set[str]) -> None:
        """Drop per-(group, detector) buckets for groups removed by a reload."""
        with self._lock:
            self._samples = {
                key: bucket for key, bucket in self._samples.items() if key[0] in names
            }

    def _prune_locked(self, bucket: deque[DetectOutcomeSample], now: float) -> None:
        cutoff = now - self._max_window
        while bucket and bucket[0].timestamp < cutoff:
            bucket.popleft()


def _ratio_in_window(
    bucket: deque[DetectOutcomeSample], now: float, window_seconds: int
) -> float | None:
    cutoff = now - window_seconds
    attempts = 0
    successes = 0
    for sample in bucket:
        if sample.timestamp < cutoff:
            continue
        attempts += sample.attempts
        successes += sample.successes
    if attempts == 0:
        return None
    return successes / attempts


def estimate_samples_bytes(samples: Iterable[object]) -> float:
    """Best-effort ``sys.getsizeof`` sum of a group's output samples.

    Walks the dataclass ``Sample`` objects and their label tuples rather
    than guessing — cheap relative to the detection run and accurate
    enough to answer "which group's snapshot is dominant". Not an exact
    process-level breakdown: shared interned strings are double-counted,
    which is acceptable for a relative gauge.
    """
    import sys

    total = 0
    for sample in samples:
        total += sys.getsizeof(sample)
        labels = getattr(sample, "labels", None)
        if labels is None:
            continue
        total += sys.getsizeof(labels)
        for pair in labels:
            total += sys.getsizeof(pair)
            for item in pair:
                total += sys.getsizeof(item)
    return float(total)


def _format_window(seconds: int) -> str:
    """Render a duration as the Prometheus-style ``<n><unit>`` label."""
    if seconds <= 0:
        return "0s"
    if seconds % 86_400 == 0:
        return f"{seconds // 86_400}d"
    if seconds % 3_600 == 0:
        return f"{seconds // 3_600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


__all__ = [
    "DEFAULT_SUCCESS_WINDOWS_SECONDS",
    "DetectOutcomeSample",
    "DetectOutcomeTracker",
    "SuccessRatioPoint",
    "estimate_samples_bytes",
]
