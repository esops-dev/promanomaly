"""Shared test fixtures + monkey-patchable PromQL source."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import pytest

from promanomaly.source import PromQLSource, QueryResult, QuerySeries


@dataclass
class StubResponse:
    series: list[QuerySeries] = field(default_factory=list)
    error: Exception | None = None


class StubSource(PromQLSource):
    """Drop-in replacement for the live PromQL client used in unit tests."""

    def __init__(self) -> None:
        super().__init__(base_url="http://stub", timeout=1.0)
        self.queries: list[dict[str, Any]] = []
        self._responder: Callable[[str], StubResponse] = lambda _: StubResponse()
        # {metric_name: type} served by ``metric_metadata`` (metadata-lint).
        # Empty by default so the lint reports nothing unless a test opts in.
        self.metadata: dict[str, str] = {}

    def respond(self, fn: Callable[[str], StubResponse]) -> None:
        self._responder = fn

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def metric_metadata(self) -> dict[str, str]:  # type: ignore[override]
        return dict(self.metadata)

    async def range_query(  # type: ignore[override]
        self,
        promql: str,
        end: float,
        window_seconds: float,
        step_seconds: float,
        max_series: int | None = None,
    ) -> QueryResult:
        self.queries.append(
            {
                "promql": promql,
                "end": end,
                "window": window_seconds,
                "step": step_seconds,
                "max_series": max_series,
            }
        )
        resp = self._responder(promql)
        if resp.error is not None:
            raise resp.error
        return QueryResult(series=list(resp.series))


def make_label_only_series(label: str, value: str) -> QuerySeries:
    """Discovery probes only need a label set; the sample frame is unused."""
    return QuerySeries(
        labels={label: value},
        samples=pd.DataFrame({"timestamp": [0.0], "y": [0.0]}),
    )


@pytest.fixture
def stub_source() -> StubSource:
    return StubSource()


def make_series(labels: dict[str, str], samples: pd.DataFrame) -> QuerySeries:
    return QuerySeries(labels=labels, samples=samples)
