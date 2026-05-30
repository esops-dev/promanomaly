"""Grafana annotations sink.

Posts an annotation to the Grafana HTTP API on each
``anomaly_outside_threshold`` 0->1 transition and each
``anomaly_change_point_total`` increment, so anomalies surface across
*all* dashboards (Grafana renders tag-matched annotations globally),
even ones that never query promanomaly metrics directly.

Transition detection is in-memory and stateless across restarts: the
previous per-series value is held on the instance and rebuilt from the
next run. On a cold start, series already firing are recorded as a
baseline and do *not* annotate — only observed 0->1 edges do — so a
restart can't replay a storm of stale firings into every dashboard.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import structlog

from ..config import GrafanaAnnotationsSinkConfig
from ..exporter import OperationalMetrics
from ..httpauth import build_httpx_auth
from ..state import GroupSnapshot, Sample
from .base import (
    REASON_CONNECT,
    REASON_HTTP,
    REASON_RATE_LIMITED,
    REASON_TIMEOUT,
    Sink,
)

logger = structlog.get_logger(__name__)

_OUTSIDE_METRIC = "anomaly_outside_threshold"
_CHANGE_POINT_METRIC = "anomaly_change_point_total"
_ANNOTATIONS_PATH = "/api/annotations"

# Labels lifted onto annotation tags (Grafana matches dashboards by tag).
_TAG_LABELS = ("id", "group", "detector", "detector_instance")


class GrafanaAnnotationsSink(Sink):
    """Posts firing / change-point events to the Grafana annotations API."""

    name = "grafana_annotations"

    def __init__(
        self, config: GrafanaAnnotationsSinkConfig, operational: OperationalMetrics
    ) -> None:
        super().__init__(operational)
        self._config = config
        self._client: httpx.AsyncClient | None = None
        # series identity -> last seen value. ``None`` (absent key) means
        # never observed, so the first sighting records a baseline without
        # annotating — a cold start can't replay stale firings.
        self._last_outside: dict[tuple[tuple[str, str], ...], float] = {}
        self._last_change_point: dict[tuple[tuple[str, str], ...], float] = {}

    async def start(self) -> None:
        if self._client is not None:
            return
        kwargs: dict[str, Any] = {
            "timeout": self._config.timeout_seconds,
            "base_url": self._config.url.rstrip("/"),
        }
        # Bearer token resolved via the shared helper so a Secret mounted at
        # ``token_file`` (the path the Helm chart wires) works identically to
        # an inline token. Config validation already constrains the auth type
        # to none / bearer, so only the bearer branch can fire here.
        kwargs.update(build_httpx_auth(self._config.auth))
        self._client = httpx.AsyncClient(**kwargs)

    async def emit(self, snapshot: GroupSnapshot) -> None:
        if self._client is None:
            await self.start()
        assert self._client is not None
        events = self._collect_events(snapshot)
        if not events:
            return
        budget = self._config.max_annotations_per_run
        if len(events) > budget:
            # Drop the overflow rather than flood Grafana; count it so the
            # operator can alert on a rate-limited fleet-wide event storm.
            dropped = len(events) - budget
            self._count_failure(REASON_RATE_LIMITED)
            logger.warning(
                "grafana_annotations_rate_limited",
                group=snapshot.group,
                dropped=dropped,
                budget=budget,
            )
            events = events[:budget]
        # Post concurrently — bounded by max_annotations_per_run, so this
        # never opens more than ``budget`` in-flight requests. Each _post
        # swallows and counts its own failures, so gather never raises.
        await asyncio.gather(*(self._post(event, snapshot.group) for event in events))

    def _collect_events(self, snapshot: GroupSnapshot) -> list[dict[str, Any]]:
        """Diff this snapshot against the previous to find annotatable edges."""
        timestamp_ms = int(snapshot.timestamp * 1000)
        events: list[dict[str, Any]] = []
        seen_outside: set[tuple[tuple[str, str], ...]] = set()
        seen_change_point: set[tuple[tuple[str, str], ...]] = set()
        for sample in snapshot.samples:
            if sample.metric == _OUTSIDE_METRIC:
                seen_outside.add(sample.labels)
                event = self._outside_event(sample, timestamp_ms)
            elif sample.metric == _CHANGE_POINT_METRIC:
                seen_change_point.add(sample.labels)
                event = self._change_point_event(sample, timestamp_ms)
            else:
                continue
            if event is not None:
                events.append(event)
        # Bound the transition-tracking state: drop entries for *this* group
        # whose series was absent from this run, so a workload that churns
        # through ephemeral series identities (discovery) can't grow the
        # dicts without bound. Keys for other groups are untouched — emit()
        # is called once per group. A series that vanishes and later
        # reappears firing re-baselines (no annotation), matching the
        # cold-start guard.
        self._prune(self._last_outside, snapshot.group, seen_outside)
        self._prune(self._last_change_point, snapshot.group, seen_change_point)
        return events

    @staticmethod
    def _prune(
        state: dict[tuple[tuple[str, str], ...], float],
        group: str,
        seen: set[tuple[tuple[str, str], ...]],
    ) -> None:
        stale = [key for key in state if dict(key).get("group") == group and key not in seen]
        for key in stale:
            del state[key]

    def _outside_event(self, sample: Sample, timestamp_ms: int) -> dict[str, Any] | None:
        key = sample.labels
        previous = self._last_outside.get(key)
        self._last_outside[key] = sample.value
        # First sighting establishes a baseline (no annotation); only an
        # observed 0->1 edge between two runs annotates.
        if previous is not None and previous < 1.0 <= sample.value:
            labels = dict(sample.labels)
            return {
                "time": timestamp_ms,
                "tags": self._tags(labels),
                "text": self._describe(labels, "anomaly fired (outside threshold)"),
            }
        return None

    def _change_point_event(self, sample: Sample, timestamp_ms: int) -> dict[str, Any] | None:
        key = sample.labels
        previous = self._last_change_point.get(key)
        self._last_change_point[key] = sample.value
        # First sighting establishes a baseline; a counter reset (restart)
        # lowers the value and is not treated as an increment.
        if previous is not None and sample.value > previous:
            labels = dict(sample.labels)
            return {
                "time": timestamp_ms,
                "tags": self._tags(labels),
                "text": self._describe(labels, "change-point detected"),
            }
        return None

    async def _post(self, event: dict[str, Any], group: str) -> None:
        assert self._client is not None
        try:
            response = await self._client.post(_ANNOTATIONS_PATH, json=event)
            response.raise_for_status()
        except httpx.TimeoutException:
            logger.warning("grafana_annotations_timeout", group=group)
            self._count_failure(REASON_TIMEOUT)
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "grafana_annotations_http_error", group=group, status=exc.response.status_code
            )
            self._count_failure(REASON_HTTP)
        except httpx.HTTPError as exc:
            logger.warning("grafana_annotations_connect_error", group=group, error=str(exc))
            self._count_failure(REASON_CONNECT)

    def _tags(self, labels: dict[str, str]) -> list[str]:
        tags = list(self._config.tags)
        for name in _TAG_LABELS:
            value = labels.get(name)
            if value:
                tags.append(f"{name}:{value}")
        return tags

    @staticmethod
    def _describe(labels: dict[str, str], what: str) -> str:
        ident = labels.get("id", "?")
        group = labels.get("group", "?")
        detector = labels.get("detector")
        suffix = f" [{detector}]" if detector else ""
        return f"promanomaly: {what} — {group}/{ident}{suffix}"

    async def aclose(self) -> None:
        if self._client is None:
            return
        try:
            await self._client.aclose()
        finally:
            self._client = None


__all__ = ["GrafanaAnnotationsSink"]
