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
