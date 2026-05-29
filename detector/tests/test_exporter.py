"""Exporter / metric-name-validator tests."""

from __future__ import annotations

import pytest

from promanomaly.exporter import (
    InvalidMetricNameError,
    render_label_value,
    render_samples,
    validate_label_name,
    validate_metric_name,
)
from promanomaly.state import Sample


def test_metric_name_colon_rejected() -> None:
    with pytest.raises(InvalidMetricNameError, match="reserved"):
        validate_metric_name("namespace:metric")


def test_metric_name_invalid_chars_rejected() -> None:
    with pytest.raises(InvalidMetricNameError):
        validate_metric_name("with space")


def test_metric_name_valid_accepted() -> None:
    assert validate_metric_name("anomaly_score") == "anomaly_score"


def test_label_name_colon_rejected() -> None:
    with pytest.raises(InvalidMetricNameError):
        validate_label_name("env:prod")


def test_render_samples_groups_by_metric() -> None:
    samples = [
        Sample(metric="anomaly_score", labels=(("id", "a"), ("group", "g")), value=4.2),
        Sample(metric="anomaly_score", labels=(("id", "b"), ("group", "g")), value=0.0),
        Sample(metric="anomaly_baseline", labels=(("id", "a"), ("group", "g")), value=10.0),
    ]
    out = render_samples(samples)
    assert "# TYPE anomaly_score gauge" in out
    assert "# TYPE anomaly_baseline gauge" in out
    assert 'anomaly_score{id="a",group="g"} 4.2' in out
    assert 'anomaly_baseline{id="a",group="g"} 10.0' in out


def test_label_escaping() -> None:
    assert render_label_value('a"b\\c') == 'a\\"b\\\\c'
    assert render_label_value("line\nbreak") == "line\\nbreak"


def test_render_rejects_colon_in_metric() -> None:
    samples = [Sample(metric="bad:name", labels=(), value=1.0)]
    with pytest.raises(InvalidMetricNameError):
        render_samples(samples)


def test_exporter_caches_rendered_snapshot_bytes() -> None:
    """The render cache keys on ``(group, snapshot.timestamp)`` so two
    consecutive scrapes against an unchanged snapshot reuse the same
    bytes object without re-running per-sample regex validation. A new
    snapshot (different timestamp) busts the cache and re-renders."""
    from promanomaly.exporter import Exporter, OperationalMetrics
    from promanomaly.state import GroupSnapshot, SnapshotStore

    store = SnapshotStore()
    ops = OperationalMetrics()
    exporter = Exporter(store, ops, source=None)

    samples = [
        Sample(metric="anomaly_score", labels=(("group", "g"), ("id", "a")), value=1.0),
    ]
    store.write(GroupSnapshot(group="g", timestamp=100.0, samples=samples))

    exporter.render()
    cached_bytes = exporter._snapshot_render_bytes  # type: ignore[reportPrivateUsage]
    exporter.render()
    # Second render reuses the cached *snapshot* bytes (no re-render, no
    # new alloc). The cache-bytes identity is the real assertion — the
    # full body legitimately differs scrape-to-scrape now that the
    # time-relative self-observability gauges (snapshot_age) re-render
    # per scrape.
    assert exporter._snapshot_render_bytes is cached_bytes  # type: ignore[reportPrivateUsage]

    # New snapshot at a new timestamp busts the cache and the bytes
    # object differs.
    store.write(GroupSnapshot(group="g", timestamp=200.0, samples=samples))
    exporter.render()
    assert exporter._snapshot_render_bytes is not cached_bytes  # type: ignore[reportPrivateUsage]


def test_query_variant_delegates_attribute_reads_to_underlying_config() -> None:
    """``_QueryVariant`` overrides ``id`` and ``promql`` for discover
    fan-out and delegates every other field back to the original
    ``QueryConfig``. The wrapper exists to avoid ``pydantic.model_copy``
    on every variant — at hundreds of fan-outs per refresh that was the
    dominant per-tick allocation cost. Test pins the contract so a
    future schema addition cannot silently break delegation."""
    from promanomaly.config import QueryConfig
    from promanomaly.runner import _QueryVariant

    base = QueryConfig.model_validate(
        {
            "id": "cpu",
            "promql": "rate(cpu[5m])",
            "expect": True,
            "expect_grace_runs": 5,
            "detectors": [{"name": "MAD"}],
        }
    )
    variant = _QueryVariant(
        base, rendered_id="cpu_node17", rendered_promql='rate(cpu{node="17"}[5m])'
    )

    # Overrides win for id/promql.
    assert variant.id == "cpu_node17"
    assert variant.promql == 'rate(cpu{node="17"}[5m])'
    # Every other field reads through to the underlying QueryConfig.
    assert variant.expect is True
    assert variant.expect_grace_runs == 5
    assert variant.detectors == base.detectors
    # And nested attribute access still works (no eager unwrapping).
    assert variant.detectors[0].name == "MAD"
