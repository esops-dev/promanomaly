"""Tests for dynamic series discovery (v0.4).

Covers:

- Pure :func:`promanomaly.discovery.expand_query` rendering and the
  ``DiscoveryError`` paths that the runner reads to skip a query.
- Runner-level expansion of a query that declares a ``discover:`` block.
- Discovery-based ``anomaly_signal_missing`` emission when a previously
  seen series disappears for ``expect_grace_runs`` consecutive runs —
  the dynamic counterpart to the v0.1 static ``expect:`` flag.
"""

from __future__ import annotations

from typing import Any

import pytest

from promanomaly.config import CURRENT_API_VERSION, Config, QueryConfig
from promanomaly.discovery import DiscoveryError, ExpandedQuery, expand_query
from promanomaly.exporter import OperationalMetrics
from promanomaly.runner import Runner
from promanomaly.state import SnapshotStore
from tests.conftest import StubResponse, StubSource, make_label_only_series, make_series
from tests.fixtures import clean_baseline


def _query_with_discover(
    *,
    id_template: str = "node_cpu_{{ instance }}",
    promql_template: str = 'rate(node_cpu_seconds_total{instance="{{ instance }}"}[1m])',
    probe: str = "up{job='node'}",
    label: str = "instance",
    grace_runs: int = 2,
) -> QueryConfig:
    return QueryConfig.model_validate(
        {
            "id": id_template,
            "promql": promql_template,
            "expect_grace_runs": grace_runs,
            "discover": [{"variable": "instance", "probe": probe, "label": label}],
            "detectors": [{"name": "MAD"}],
        }
    )


# ----------------------------------------------------------------------
# expand_query / DiscoveryError
# ----------------------------------------------------------------------


def test_expand_query_renders_cartesian_combinations() -> None:
    query = QueryConfig.model_validate(
        {
            "id": "node_{{ instance }}_{{ zone }}",
            "promql": 'up{instance="{{ instance }}", zone="{{ zone }}"}',
            "discover": [
                {"variable": "instance", "probe": "x", "label": "instance"},
                {"variable": "zone", "probe": "y", "label": "zone"},
            ],
            "detectors": [{"name": "MAD"}],
        }
    )
    variants = expand_query(
        query,
        {
            "instance": ["a", "b"],
            "zone": ["us", "eu"],
        },
    )
    rendered_ids = sorted(v.rendered_id for v in variants)
    assert rendered_ids == ["node_a_eu", "node_a_us", "node_b_eu", "node_b_us"]
    # Discovered labels carry the binding so the runner can attach them
    # to anomaly_signal_missing on absence.
    for v in variants:
        assert set(v.labels.keys()) == {"instance", "zone"}


def test_expand_query_rejects_undeclared_placeholder() -> None:
    query = QueryConfig.model_validate(
        {
            "id": "node_{{ instance }}_{{ undeclared }}",
            "promql": "up",
            "discover": [
                {"variable": "instance", "probe": "x", "label": "instance"},
            ],
            "detectors": [{"name": "MAD"}],
        }
    )
    with pytest.raises(DiscoveryError) as excinfo:
        expand_query(query, {"instance": ["a"]})
    assert excinfo.value.reason == "undeclared_variable"


def test_expand_query_rejects_invalid_rendered_id() -> None:
    # Discovery values must not introduce Prometheus-illegal characters
    # into the rendered metric id. "in-stance" contains a hyphen.
    query = QueryConfig.model_validate(
        {
            "id": "node_{{ instance }}",
            "promql": "up",
            "discover": [
                {"variable": "instance", "probe": "x", "label": "instance"},
            ],
            "detectors": [{"name": "MAD"}],
        }
    )
    with pytest.raises(DiscoveryError) as excinfo:
        expand_query(query, {"instance": ["in-stance"]})
    assert excinfo.value.reason == "invalid_rendered_id"


def test_expand_query_returns_empty_when_any_variable_empty() -> None:
    query = QueryConfig.model_validate(
        {
            "id": "node_{{ instance }}",
            "promql": "up",
            "discover": [
                {"variable": "instance", "probe": "x", "label": "instance"},
            ],
            "detectors": [{"name": "MAD"}],
        }
    )
    assert expand_query(query, {"instance": []}) == []


def test_is_safe_discovered_value_rejects_quoting_breakers() -> None:
    from promanomaly.discovery import is_safe_discovered_value

    assert is_safe_discovered_value("node_a")
    assert is_safe_discovered_value("us-east-1")  # hyphens are ok inside label values
    assert not is_safe_discovered_value('foo"bar')
    assert not is_safe_discovered_value("foo\\bar")
    assert not is_safe_discovered_value("line\nbreak")


def test_render_template_substitutes_bindings() -> None:
    from promanomaly.discovery import render_template

    assert render_template("a_{{ x }}_b", {"x": "1"}) == "a_1_b"
    assert render_template("{{ a }}_{{ b }}", {"a": "left", "b": "right"}) == "left_right"
    # Unbound placeholders pass through verbatim — caller decides.
    assert render_template("{{ unbound }}", {}) == "{{ unbound }}"


def test_expand_query_no_discover_returns_query_unchanged() -> None:
    query = QueryConfig.model_validate(
        {
            "id": "plain",
            "promql": "up",
            "detectors": [{"name": "MAD"}],
        }
    )
    variants = expand_query(query, {})
    assert variants == [ExpandedQuery(rendered_id="plain", rendered_promql="up", labels={})]


# ----------------------------------------------------------------------
# Runner integration
# ----------------------------------------------------------------------


def _config_with_discovery(
    *,
    grace_runs: int = 2,
) -> Config:
    return Config.model_validate(
        {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://stub"},
            "defaults": {
                "window": "5m",
                "step": "15s",
                "min_points": 5,
                "warmup_policy": "emit_warming_up",
                "emit_baseline_stability": False,
                "alert_thresholds": {"score": 3.0},
            },
            "groups": [
                {
                    "name": "g1",
                    "queries": [
                        _query_with_discover(grace_runs=grace_runs).model_dump(),
                    ],
                }
            ],
        }
    )


def _components(cfg: Config, stub: StubSource) -> tuple[Runner, SnapshotStore, OperationalMetrics]:
    store = SnapshotStore()
    ops = OperationalMetrics()
    return Runner(cfg, stub, store, ops), store, ops


@pytest.mark.asyncio
async def test_runner_expands_discovery_into_per_instance_series(
    stub_source: StubSource,
) -> None:
    cfg = _config_with_discovery()
    runner, _store, ops = _components(cfg, stub_source)

    probe_text = "up{job='node'}"

    def respond(promql: str) -> StubResponse:
        if promql == probe_text:
            return StubResponse(
                series=[
                    make_label_only_series("instance", "node_a"),
                    make_label_only_series("instance", "node_b"),
                ]
            )
        # rendered query: serve a clean baseline so scoring succeeds
        return StubResponse(series=[make_series({"instance": "x"}, clean_baseline(n=120))])

    stub_source.respond(respond)

    result = await runner.run_group("g1")
    assert result.succeeded
    # One probe + one rendered fetch per discovered instance = 3.
    promqls = [q["promql"] for q in stub_source.queries]
    assert probe_text in promqls
    assert any('instance="node_a"' in p for p in promqls)
    assert any('instance="node_b"' in p for p in promqls)
    # Fan-out gauge published.
    expansions = ops.registry.get_sample_value(
        "anomaly_discovery_expansions",
        {"group": "g1", "query": "node_cpu_{{ instance }}"},
    )
    assert expansions == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_runner_emits_signal_missing_when_series_disappears(
    stub_source: StubSource,
) -> None:
    cfg = _config_with_discovery(grace_runs=2)
    runner, _store, ops = _components(cfg, stub_source)

    probe_text = "up{job='node'}"
    state: dict[str, Any] = {"instances": ["node_a", "node_b"]}

    def respond(promql: str) -> StubResponse:
        if promql == probe_text:
            return StubResponse(
                series=[make_label_only_series("instance", i) for i in state["instances"]]
            )
        return StubResponse(series=[make_series({"instance": "x"}, clean_baseline(n=120))])

    stub_source.respond(respond)

    first = await runner.run_group("g1")
    assert first.succeeded
    # Both instances seen — no signal_missing yet.
    assert all(s.metric != "anomaly_signal_missing" for s in first.samples)

    # node_b vanishes on the next two runs. After grace_runs=2 the
    # signal_missing metric should appear for node_b only.
    state["instances"] = ["node_a"]
    second = await runner.run_group("g1")
    assert all(s.metric != "anomaly_signal_missing" for s in second.samples)
    third = await runner.run_group("g1")
    missing = [s for s in third.samples if s.metric == "anomaly_signal_missing"]
    assert len(missing) == 1
    labels = dict(missing[0].labels)
    # Rendered id: matches the id the series had when it was present
    # so operators alerting on anomaly_signal_missing{id="node_cpu_node_b"}
    # actually fires. Bug #1: emitting the template id here breaks that.
    assert labels["id"] == "node_cpu_node_b"
    assert labels["group"] == "g1"
    assert labels["instance"] == "node_b"
    # Counter bumped exactly once on the transition.
    counter = ops.registry.get_sample_value(
        "anomaly_signal_missing_total",
        {"group": "g1", "reason": "missing_grace_exhausted"},
    )
    assert counter == pytest.approx(1.0)

    # When node_b comes back, signal_missing stops being emitted.
    state["instances"] = ["node_a", "node_b"]
    fourth = await runner.run_group("g1")
    assert all(s.metric != "anomaly_signal_missing" for s in fourth.samples)


@pytest.mark.asyncio
async def test_runner_probe_failure_preserves_snapshot_under_serve_stale(
    stub_source: StubSource,
) -> None:
    """Probe failure must NOT silently wipe the previous snapshot.

    Bug #2: under the default ``serve_stale`` policy a discovery probe
    that hits a TSDB error used to log + return, leaving the run to
    succeed with empty samples — overwriting the last good snapshot
    with nothing. The fix is to treat probe-source failure the same
    way as a regular query source failure: bump the counter, propagate,
    let ``run_group`` preserve the snapshot.
    """
    from promanomaly.source import SourceQueryError

    cfg = _config_with_discovery()
    runner, store, ops = _components(cfg, stub_source)

    probe_text = "up{job='node'}"
    state: dict[str, Any] = {"healthy": True}

    def respond(promql: str) -> StubResponse:
        if promql == probe_text:
            if state["healthy"]:
                return StubResponse(series=[make_label_only_series("instance", "node_a")])
            return StubResponse(error=SourceQueryError("timeout", "probe timed out"))
        return StubResponse(series=[make_series({"instance": "x"}, clean_baseline(n=120))])

    stub_source.respond(respond)

    # First run: healthy, populates the snapshot.
    first = await runner.run_group("g1")
    assert first.succeeded
    snap_before = store.get("g1")
    assert snap_before is not None
    assert len(snap_before.samples) > 0

    # Second run: probe fails. Under serve_stale the group run must be
    # marked failed (so snapshot is preserved) and the source-failure
    # counter must bump.
    state["healthy"] = False
    second = await runner.run_group("g1")
    assert not second.succeeded
    snap_after = store.get("g1")
    assert snap_after is not None
    # Snapshot was NOT overwritten with empty data.
    assert snap_after.samples == snap_before.samples
    src_failures = ops.registry.get_sample_value(
        "anomaly_source_failure_total",
        {"group": "g1", "reason": "timeout"},
    )
    assert src_failures == pytest.approx(1.0)
    disc_failures = ops.registry.get_sample_value(
        "anomaly_discovery_failures_total",
        {
            "group": "g1",
            "query": "node_cpu_{{ instance }}",
            "variable": "instance",
            "reason": "probe_query_failed",
        },
    )
    assert disc_failures == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_runner_filters_unsafe_discovered_values_and_bumps_counter(
    stub_source: StubSource,
) -> None:
    """Discovered values that would break PromQL quoting (containing ``"``,
    ``\\``, or newlines) must be filtered before substitution.

    Bug #3: naive substitution of ``foo"bar`` into ``instance="{{ x }}"``
    yields ``instance="foo"bar"`` — broken PromQL and a potential
    label-injection vector. The runner drops the unsafe values, logs
    a warning, and bumps ``anomaly_discovery_failures_total`` with
    reason=unsafe_value so operators can spot it on dashboards.
    """
    cfg = _config_with_discovery()
    runner, _store, ops = _components(cfg, stub_source)

    probe_text = "up{job='node'}"

    def respond(promql: str) -> StubResponse:
        if promql == probe_text:
            return StubResponse(
                series=[
                    make_label_only_series("instance", "node_a"),
                    make_label_only_series("instance", 'evil"label'),
                ]
            )
        return StubResponse(series=[make_series({"instance": "x"}, clean_baseline(n=120))])

    stub_source.respond(respond)

    result = await runner.run_group("g1")
    assert result.succeeded
    # Only node_a expanded.
    expansions = ops.registry.get_sample_value(
        "anomaly_discovery_expansions",
        {"group": "g1", "query": "node_cpu_{{ instance }}"},
    )
    assert expansions == pytest.approx(1.0)
    failures = ops.registry.get_sample_value(
        "anomaly_discovery_failures_total",
        {
            "group": "g1",
            "query": "node_cpu_{{ instance }}",
            "variable": "instance",
            "reason": "unsafe_value",
        },
    )
    assert failures == pytest.approx(1.0)
    # No PromQL containing the unsafe value was ever sent to the source.
    promqls = [q["promql"] for q in stub_source.queries]
    assert all('evil"label' not in p for p in promqls)


@pytest.mark.asyncio
async def test_runner_forgets_long_missing_discovered_series(
    stub_source: StubSource,
) -> None:
    """Bug #4: a workload that churns through ephemeral identities
    would otherwise grow the discovery-tracker dict without bound.

    With ``safety.discovery.forget_after_runs`` set, a series that has
    been missing for that many consecutive runs is dropped from the
    tracker; ``anomaly_signal_missing`` stops being emitted for it, and
    if it ever reappears it counts as a fresh discovery.
    """
    from promanomaly.config import CURRENT_API_VERSION, Config

    cfg = Config.model_validate(
        {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://stub"},
            "safety": {"discovery": {"forget_after_runs": 2}},
            "defaults": {
                "window": "5m",
                "step": "15s",
                "min_points": 5,
                "warmup_policy": "emit_warming_up",
                "emit_baseline_stability": False,
                "alert_thresholds": {"score": 3.0},
            },
            "groups": [
                {
                    "name": "g1",
                    "queries": [
                        _query_with_discover(grace_runs=1).model_dump(),
                    ],
                }
            ],
        }
    )
    runner, store, _ops = _components(cfg, stub_source)

    probe_text = "up{job='node'}"
    state: dict[str, Any] = {"instances": ["node_a"]}

    def respond(promql: str) -> StubResponse:
        if promql == probe_text:
            return StubResponse(
                series=[make_label_only_series("instance", i) for i in state["instances"]]
            )
        return StubResponse(series=[make_series({"instance": "x"}, clean_baseline(n=120))])

    stub_source.respond(respond)

    # Run once with node_a present — it gets remembered.
    await runner.run_group("g1")
    assert ("instance", "node_a") in {
        next(iter(s)) for s in store.discovery_known_series("g1", "node_cpu_{{ instance }}")
    }

    # node_a vanishes. Each run increments its miss count. After
    # forget_after_runs=2 the tracker drops it entirely.
    state["instances"] = []
    await runner.run_group("g1")  # miss=1 -> grace=1 trips, signal_missing emitted
    await runner.run_group("g1")  # miss=2 -> still tracked
    await runner.run_group("g1")  # miss=3 > forget_after_runs -> dropped

    assert store.discovery_known_series("g1", "node_cpu_{{ instance }}") == set()


@pytest.mark.asyncio
async def test_runner_drop_scores_swallows_probe_failure_but_keeps_run_succeeded(
    stub_source: StubSource,
) -> None:
    """Under ``drop_scores`` a probe failure is logged and the run
    proceeds — same semantics as a regular query failure under that
    policy. The counter still bumps."""
    from promanomaly.config import CURRENT_API_VERSION, Config
    from promanomaly.source import SourceQueryError

    cfg = Config.model_validate(
        {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://stub"},
            "safety": {"on_source_failure": "drop_scores"},
            "defaults": {
                "window": "5m",
                "step": "15s",
                "min_points": 5,
                "warmup_policy": "emit_warming_up",
                "emit_baseline_stability": False,
                "alert_thresholds": {"score": 3.0},
            },
            "groups": [
                {
                    "name": "g1",
                    "queries": [
                        _query_with_discover().model_dump(),
                        # A second always-empty regular query so the
                        # group isn't 100% probe failure (which would
                        # degrade to serve_stale under drop_scores).
                        {
                            "id": "fallback",
                            "promql": "up",
                            "detectors": [{"name": "MAD"}],
                        },
                    ],
                }
            ],
        }
    )
    runner, _store, ops = _components(cfg, stub_source)

    probe_text = "up{job='node'}"

    def respond(promql: str) -> StubResponse:
        if promql == probe_text:
            return StubResponse(error=SourceQueryError("timeout", "probe timed out"))
        return StubResponse(series=[])

    stub_source.respond(respond)

    result = await runner.run_group("g1")
    assert result.succeeded
    assert result.source_failures == 1
    src_failures = ops.registry.get_sample_value(
        "anomaly_source_failure_total",
        {"group": "g1", "reason": "timeout"},
    )
    assert src_failures == pytest.approx(1.0)
