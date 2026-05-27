"""Output-label allow/deny filtering at the exporter boundary.

End-to-end tests: drive a real Runner with a stub source carrying
sensitive source labels and assert the rendered output respects the
``exporter.output_labels`` allow / drop config. Schema-level rejection
of canonical labels in ``drop`` is also exercised so misconfiguration
fails at validate time, not in production.
"""

from __future__ import annotations

from typing import Any

import pytest

from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_series
from tests.fixtures import point_spike


def _config(*, allow: list[str] | None = None, drop: list[str] | None = None) -> Config:
    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
            "alert_thresholds": {"score": 3.0},
        },
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {"id": "metric_a", "promql": "up{job='x'}", "detectors": [{"name": "MAD"}]},
                ],
            }
        ],
    }
    if allow is not None or drop is not None:
        raw["exporter"] = {
            "output_labels": {
                "allow": allow or [],
                "drop": drop or [],
            }
        }
    return Config.model_validate(raw)


@pytest.mark.asyncio
async def test_no_filter_passes_every_source_label(stub_source: StubSource) -> None:
    df = point_spike(n=60)
    labels = {"instance": "host-a", "customer_id": "acme-123", "region": "eu-west-1"}
    stub_source.respond(lambda _: StubResponse(series=[make_series(labels, df)]))
    cfg = _config()  # no filter
    runner = Runner(cfg, stub_source, SnapshotStore(), OperationalMetrics())

    result = await runner.run_group("g1")
    score = next(s for s in result.samples if s.metric == "anomaly_score")
    for label in labels:
        assert label in score.label_dict


@pytest.mark.asyncio
async def test_drop_strips_listed_source_labels(stub_source: StubSource) -> None:
    df = point_spike(n=60)
    labels = {"instance": "host-a", "customer_id": "acme-123", "region": "eu-west-1"}
    stub_source.respond(lambda _: StubResponse(series=[make_series(labels, df)]))
    cfg = _config(drop=["customer_id"])
    runner = Runner(cfg, stub_source, SnapshotStore(), OperationalMetrics())

    result = await runner.run_group("g1")
    score = next(s for s in result.samples if s.metric == "anomaly_score")
    # Canonical labels still flow through.
    assert score.label_dict["id"] == "metric_a"
    assert score.label_dict["group"] == "g1"
    assert score.label_dict["detector"] == "MAD"
    # ``customer_id`` was filtered out; the other source labels are intact.
    assert "customer_id" not in score.label_dict
    assert score.label_dict["instance"] == "host-a"
    assert score.label_dict["region"] == "eu-west-1"


@pytest.mark.asyncio
async def test_allow_restricts_to_listed_source_labels(stub_source: StubSource) -> None:
    df = point_spike(n=60)
    labels = {"instance": "host-a", "customer_id": "acme-123", "region": "eu-west-1"}
    stub_source.respond(lambda _: StubResponse(series=[make_series(labels, df)]))
    cfg = _config(allow=["instance"])
    runner = Runner(cfg, stub_source, SnapshotStore(), OperationalMetrics())

    result = await runner.run_group("g1")
    score = next(s for s in result.samples if s.metric == "anomaly_score")
    assert score.label_dict["instance"] == "host-a"
    # Canonical labels stay regardless of allow filter.
    assert "id" in score.label_dict
    assert "group" in score.label_dict
    assert "detector" in score.label_dict
    # Non-allowed source labels are dropped.
    assert "customer_id" not in score.label_dict
    assert "region" not in score.label_dict


@pytest.mark.asyncio
async def test_drop_takes_precedence_over_allow(stub_source: StubSource) -> None:
    df = point_spike(n=60)
    labels = {"instance": "host-a", "customer_id": "acme-123"}
    stub_source.respond(lambda _: StubResponse(series=[make_series(labels, df)]))
    cfg = _config(allow=["instance", "customer_id"], drop=["customer_id"])
    runner = Runner(cfg, stub_source, SnapshotStore(), OperationalMetrics())

    result = await runner.run_group("g1")
    score = next(s for s in result.samples if s.metric == "anomaly_score")
    assert score.label_dict["instance"] == "host-a"
    # ``customer_id`` was both allowed and dropped — drop wins.
    assert "customer_id" not in score.label_dict


@pytest.mark.asyncio
async def test_filter_applies_to_composite_metrics(stub_source: StubSource) -> None:
    df = point_spike(n=60)
    labels = {"instance": "host-a", "customer_id": "acme-123"}
    stub_source.respond(lambda _: StubResponse(series=[make_series(labels, df)]))
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 30,
            "alert_thresholds": {"score": 3.0},
        },
        "exporter": {"output_labels": {"drop": ["customer_id"]}},
        "groups": [
            {
                "name": "g1",
                "ensemble": {"method": "max"},
                "queries": [
                    {
                        "id": "metric_a",
                        "promql": "up{job='x'}",
                        "detectors": [{"name": "MAD"}, {"name": "Hampel"}],
                    },
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    runner = Runner(cfg, stub_source, SnapshotStore(), OperationalMetrics())

    result = await runner.run_group("g1")
    composite = next(s for s in result.samples if s.metric == "anomaly_composite_score")
    assert "customer_id" not in composite.label_dict
    assert composite.label_dict["instance"] == "host-a"


@pytest.mark.asyncio
async def test_drop_filters_discovery_signal_missing_labels(
    stub_source: StubSource,
) -> None:
    # Regression: when a discovery probe surfaces a label the operator
    # has listed in ``exporter.output_labels.drop`` (e.g. customer_id),
    # the absence signal must strip it just like the scored metrics do.
    # Before the fix, anomaly_signal_missing carried the raw discovered
    # label set verbatim, leaking the customer id even when the rest of
    # the pipeline filtered it.
    raw = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "defaults": {
            "window": "15m",
            "step": "15s",
            "min_points": 1,
            "alert_thresholds": {"score": 3.0},
        },
        "exporter": {"output_labels": {"drop": ["customer_id"]}},
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "rev_{{ customer_id }}",
                        "promql": 'sum(revenue{customer_id="{{ customer_id }}"})',
                        "expect_grace_runs": 1,
                        "discover": [
                            {
                                "variable": "customer_id",
                                "probe": "count by (customer_id) (revenue)",
                                "label": "customer_id",
                            }
                        ],
                        "detectors": [{"name": "MAD"}],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)

    # First run: discovery sees customer_id=acme. Probe returns acme;
    # the rendered query returns one series so MAD can score.
    from promanomaly.exporter import OperationalMetrics
    from promanomaly.runner import Runner
    from promanomaly.state import SnapshotStore
    from tests.conftest import make_label_only_series
    from tests.fixtures import clean_baseline

    store = SnapshotStore()

    def _first_run(promql: str) -> StubResponse:
        if "count by" in promql:
            return StubResponse(series=[make_label_only_series("customer_id", "acme")])
        return StubResponse(series=[make_series({"customer_id": "acme"}, clean_baseline(n=60))])

    stub_source.respond(_first_run)
    runner = Runner(cfg, stub_source, store, OperationalMetrics())
    await runner.run_group("g1")

    # Second run: probe no longer surfaces acme (the customer went
    # away). Discovery's miss counter trips on the first absence
    # because expect_grace_runs is 1; signal_missing should fire with
    # the customer_id stripped per the exporter filter.
    def _second_run(promql: str) -> StubResponse:
        if "count by" in promql:
            return StubResponse(series=[])
        return StubResponse(series=[])

    stub_source.respond(_second_run)
    result = await runner.run_group("g1")
    missing = [s for s in result.samples if s.metric == "anomaly_signal_missing"]
    assert missing, "expected at least one anomaly_signal_missing sample"
    for sample in missing:
        assert "customer_id" not in sample.label_dict, (
            f"customer_id leaked through filter on {sample.metric}: {sample.label_dict}"
        )
        # The rendered id still carries the discovered value because
        # ``id`` is canonical and the operator explicitly chose to
        # template it through. Filtering ``id`` would require renaming
        # the metric itself.
        assert sample.label_dict["id"] == "rev_acme"


def test_drop_rejects_canonical_label_at_load() -> None:
    with pytest.raises(ValueError, match="cannot strip canonical detector labels"):
        _config(drop=["id"])
    with pytest.raises(ValueError, match="cannot strip canonical detector labels"):
        _config(drop=["detector_instance"])


def test_allow_listing_canonical_label_is_harmless() -> None:
    # Listing ``id`` in ``allow`` is fine — the canonical labels are
    # added after the filter anyway, so it has no effect. Operators
    # often add them defensively while testing.
    cfg = _config(allow=["id", "instance"])
    assert "id" in cfg.exporter.output_labels.allow
