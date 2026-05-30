"""Tests for the blast-radius rollup — collect_blast_radius + /debug/anomalies."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

from fastapi.testclient import TestClient

from promanomaly.anomalies import collect_blast_radius
from promanomaly.config import CURRENT_API_VERSION, Config
from promanomaly.main import Application
from promanomaly.state import GroupSnapshot, Sample, SnapshotStore
from tests.conftest import StubSource


def _store_with(group: str, samples: list[Sample]) -> SnapshotStore:
    store = SnapshotStore()
    store.write(GroupSnapshot(group=group, timestamp=time.time(), samples=samples))
    return store


def _sample(metric: str, value: float, **labels: str) -> Sample:
    return Sample(metric=metric, value=value, labels=tuple(sorted(labels.items())))


def _row(rows: list[Any], scope: str, **match: str) -> Any:
    for row in rows:
        if row.scope != scope:
            continue
        if all(getattr(row, k, None) == v for k, v in match.items()):
            return row
    raise AssertionError(f"no {scope} row matching {match} in {[r.as_dict() for r in rows]}")


def test_group_rollup_firing_total_fraction_and_duration() -> None:
    # Three members; two firing, one clear. One firing member has been
    # breached for 120s, the other 40s, so the max is 120.
    samples = [
        _sample("anomaly_outside_threshold", 1.0, id="a", group="g", detector="MAD"),
        _sample("anomaly_duration_seconds", 120.0, id="a", group="g", detector="MAD"),
        _sample("anomaly_outside_threshold", 1.0, id="b", group="g", detector="MAD"),
        _sample("anomaly_duration_seconds", 40.0, id="b", group="g", detector="MAD"),
        _sample("anomaly_outside_threshold", 0.0, id="c", group="g", detector="MAD"),
        _sample("anomaly_duration_seconds", 0.0, id="c", group="g", detector="MAD"),
    ]
    rows = collect_blast_radius(_store_with("g", samples))
    row = _row(rows, "group", group="g")
    assert row.firing == 2
    assert row.total == 3
    assert row.fraction == 2 / 3
    assert row.max_co_firing_seconds == 120.0


def test_group_rollup_prefers_density_samples_when_present() -> None:
    # The runner-emitted rollup is authoritative: active=2 over density 0.5
    # recovers a total of 4, even though only one outside_threshold sample
    # is in the snapshot.
    samples = [
        _sample("anomaly_active_series", 2.0, group="g"),
        _sample("anomaly_density", 0.5, group="g"),
        _sample("anomaly_outside_threshold", 1.0, id="a", group="g", detector="MAD"),
    ]
    rows = collect_blast_radius(_store_with("g", samples))
    row = _row(rows, "group", group="g")
    assert row.firing == 2
    assert row.total == 4
    assert row.fraction == 0.5


def test_warming_up_members_excluded() -> None:
    samples = [
        _sample("anomaly_outside_threshold", 1.0, id="a", group="g", detector="MAD"),
        _sample("anomaly_outside_threshold", 0.0, id="b", group="g", detector="MAD"),
        _sample("anomaly_warming_up", 1.0, id="b", group="g", detector="MAD"),
    ]
    rows = collect_blast_radius(_store_with("g", samples))
    row = _row(rows, "group", group="g")
    assert row.firing == 1
    assert row.total == 1
    assert row.fraction == 1.0


def test_cohort_rollup_groups_by_cohort_identity() -> None:
    # Cohort axis is "instance": three nodes in the same cohort (same job),
    # two firing. cohort_key is the shared non-axis labels.
    common = {"group": "g", "detector": "Cohort", "cohort_label": "instance", "job": "web"}
    samples = [
        _sample("anomaly_outside_threshold", 1.0, id="x", instance="n1", **common),
        _sample("anomaly_duration_seconds", 200.0, id="x", instance="n1", **common),
        _sample("anomaly_outside_threshold", 1.0, id="x", instance="n2", **common),
        _sample("anomaly_duration_seconds", 90.0, id="x", instance="n2", **common),
        _sample("anomaly_outside_threshold", 0.0, id="x", instance="n3", **common),
    ]
    rows = collect_blast_radius(_store_with("g", samples))
    cohort = _row(rows, "cohort", group="g", cohort_label="instance")
    assert cohort.firing == 2
    assert cohort.total == 3
    assert cohort.fraction == 2 / 3
    assert cohort.max_co_firing_seconds == 200.0
    # cohort_key excludes the axis label and the meta-labels.
    assert "instance" not in cohort.cohort_key
    assert cohort.cohort_key.get("job") == "web"
    assert cohort.cohort_key.get("id") == "x"


def test_empty_group_produces_no_rows() -> None:
    """A group with zero scored series produces no blast-radius rows."""
    store = SnapshotStore()
    store.write(GroupSnapshot(group="empty", timestamp=time.time(), samples=[]))
    rows = collect_blast_radius(store)
    assert rows == []


def test_all_warming_up_produces_no_rows() -> None:
    """A group where every series is warming up has no scored members.

    The density rollup excludes warming-up series, so the blast-radius
    rollup should mirror that and report nothing rather than 0/0.
    """
    samples = [
        _sample("anomaly_outside_threshold", 0.0, id="a", group="g", detector="MAD"),
        _sample("anomaly_warming_up", 1.0, id="a", group="g", detector="MAD"),
        _sample("anomaly_outside_threshold", 0.0, id="b", group="g", detector="MAD"),
        _sample("anomaly_warming_up", 1.0, id="b", group="g", detector="MAD"),
    ]
    rows = collect_blast_radius(_store_with("g", samples))
    group_rows = [r for r in rows if r.scope == "group"]
    # All members excluded by warming filter → total=0 → no row emitted,
    # consistent with the density rollup's "no fleet to measure" semantics.
    assert len(group_rows) == 1
    assert group_rows[0].total == 0
    assert group_rows[0].firing == 0
    assert group_rows[0].fraction == 0.0


def test_rows_sorted_by_fraction_then_firing() -> None:
    samples = [
        # group g1: 1 of 4 firing -> fraction 0.25
        _sample("anomaly_outside_threshold", 1.0, id="a", group="g1", detector="MAD"),
        _sample("anomaly_outside_threshold", 0.0, id="b", group="g1", detector="MAD"),
        _sample("anomaly_outside_threshold", 0.0, id="c", group="g1", detector="MAD"),
        _sample("anomaly_outside_threshold", 0.0, id="d", group="g1", detector="MAD"),
    ]
    store = SnapshotStore()
    store.write(GroupSnapshot(group="g1", timestamp=time.time(), samples=samples))
    store.write(
        GroupSnapshot(
            group="g2",
            timestamp=time.time(),
            samples=[
                _sample("anomaly_outside_threshold", 1.0, id="a", group="g2", detector="MAD"),
                _sample("anomaly_outside_threshold", 1.0, id="b", group="g2", detector="MAD"),
            ],
        ),
    )
    rows = [r for r in collect_blast_radius(store) if r.scope == "group"]
    # g2 fully firing (1.0) sorts ahead of g1 (0.25).
    assert rows[0].group == "g2"
    assert rows[1].group == "g1"


def _build_test_app() -> tuple[Any, Application]:
    import tempfile

    import yaml

    raw: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {"id": "q", "promql": "up", "detectors": [{"name": "MAD"}]},
                ],
            }
        ],
    }
    tmp_path = Path(tempfile.mkdtemp())
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(raw))
    stub = StubSource()
    with patch("promanomaly.main.PromQLSource", return_value=stub):
        application = Application(Config.model_validate(raw), config_path)
    return application.build_app(), application


def test_debug_anomalies_includes_blast_radius() -> None:
    samples = [
        _sample("anomaly_outside_threshold", 1.0, id="a", group="g", detector="MAD"),
        _sample("anomaly_severity", 0.8, id="a", group="g", detector="MAD"),
        _sample("anomaly_outside_threshold", 0.0, id="b", group="g", detector="MAD"),
    ]
    store = _store_with("g", samples)
    app, application = _build_test_app()
    application._store = store
    client = TestClient(app)
    resp = client.get("/debug/anomalies")
    assert resp.status_code == 200
    body = resp.json()
    assert "blast_radius" in body
    rows = body["blast_radius"]
    group_rows = [r for r in rows if r["scope"] == "group"]
    assert len(group_rows) == 1
    assert group_rows[0]["group"] == "g"
    assert group_rows[0]["firing"] == 1
    assert group_rows[0]["total"] == 2
    assert group_rows[0]["fraction"] == 0.5


def test_debug_anomalies_group_param_filters_blast_radius() -> None:
    store = SnapshotStore()
    store.write(
        GroupSnapshot(
            group="g",
            timestamp=time.time(),
            samples=[
                _sample("anomaly_outside_threshold", 1.0, id="a", group="g", detector="MAD"),
            ],
        ),
    )
    store.write(
        GroupSnapshot(
            group="other",
            timestamp=time.time(),
            samples=[
                _sample("anomaly_outside_threshold", 1.0, id="a", group="other", detector="MAD"),
            ],
        ),
    )
    app, application = _build_test_app()
    application._store = store
    client = TestClient(app)
    resp = client.get("/debug/anomalies?group=g")
    assert resp.status_code == 200
    rows = resp.json()["blast_radius"]
    assert {r["group"] for r in rows} == {"g"}
