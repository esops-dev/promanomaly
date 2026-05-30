"""SinkConfig validation, the sink factory, and Application wiring."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from promanomaly.config import (
    CURRENT_API_VERSION,
    Config,
    SinkConfig,
    load_config,
)
from promanomaly.exporter import OperationalMetrics
from promanomaly.sinks import (
    GrafanaAnnotationsSink,
    RemoteWriteSink,
    build_sink,
    build_sinks,
)


def _base_config(**extra: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://vm"},
        "groups": [{"name": "g1", "queries": [{"id": "q", "promql": "up", "detectors": ["MAD"]}]}],
    }
    cfg.update(extra)
    return cfg


def test_sink_requires_matching_block() -> None:
    with pytest.raises(ValueError, match=r"requires a sink\.remote_write block"):
        SinkConfig.model_validate({"type": "remote_write"})


def test_sink_rejects_unrelated_block() -> None:
    with pytest.raises(ValueError, match="configure exactly one sink"):
        SinkConfig.model_validate(
            {
                "type": "remote_write",
                "remote_write": {"url": "http://x/api/v1/write"},
                "grafana_annotations": {"url": "http://g"},
            }
        )


def test_build_sink_none_is_pull_only() -> None:
    assert build_sink(None, OperationalMetrics()) is None


def test_build_sink_remote_write() -> None:
    cfg = SinkConfig.model_validate(
        {"type": "remote_write", "remote_write": {"url": "http://x/api/v1/write"}}
    )
    sink = build_sink(cfg, OperationalMetrics())
    assert isinstance(sink, RemoteWriteSink)
    assert sink.name == "remote_write"


def test_build_sink_grafana_annotations() -> None:
    cfg = SinkConfig.model_validate(
        {"type": "grafana_annotations", "grafana_annotations": {"url": "http://g"}}
    )
    sink = build_sink(cfg, OperationalMetrics())
    assert isinstance(sink, GrafanaAnnotationsSink)


def test_config_accepts_sink_block(tmp_path: Path) -> None:
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://vm"},
        "sink": {
            "type": "remote_write",
            "remote_write": {"url": "http://vm/api/v1/write", "auth": {"type": "bearer"}},
        },
        "groups": [{"name": "g1", "queries": [{"id": "q", "promql": "up", "detectors": ["MAD"]}]}],
    }
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(cfg))
    loaded = load_config(path)
    assert loaded.sink is not None
    assert loaded.sink.type == "remote_write"


def test_config_default_sink_is_none() -> None:
    cfg = Config.model_validate(_base_config())
    assert cfg.sink is None
    assert cfg.sinks is None
    assert cfg.effective_sinks() == []


def test_effective_sinks_normalises_scalar_form() -> None:
    cfg = Config.model_validate(
        _base_config(
            sink={"type": "remote_write", "remote_write": {"url": "http://x/api/v1/write"}}
        )
    )
    effective = cfg.effective_sinks()
    assert [s.type for s in effective] == ["remote_write"]


def test_config_accepts_sinks_list() -> None:
    cfg = Config.model_validate(
        _base_config(
            sinks=[
                {"type": "remote_write", "remote_write": {"url": "http://x/api/v1/write"}},
                {"type": "grafana_annotations", "grafana_annotations": {"url": "http://g"}},
            ]
        )
    )
    assert [s.type for s in cfg.effective_sinks()] == ["remote_write", "grafana_annotations"]


def test_build_sinks_constructs_every_configured_sink() -> None:
    cfg = Config.model_validate(
        _base_config(
            sinks=[
                {"type": "remote_write", "remote_write": {"url": "http://x/api/v1/write"}},
                {"type": "grafana_annotations", "grafana_annotations": {"url": "http://g"}},
            ]
        )
    )
    sinks = build_sinks(cfg, OperationalMetrics())
    assert isinstance(sinks[0], RemoteWriteSink)
    assert isinstance(sinks[1], GrafanaAnnotationsSink)


def test_build_sinks_pull_only_is_empty() -> None:
    assert build_sinks(Config.model_validate(_base_config()), OperationalMetrics()) == []


def test_sink_and_sinks_are_mutually_exclusive() -> None:
    with pytest.raises(ValueError, match="not both"):
        Config.model_validate(
            _base_config(
                sink={"type": "remote_write", "remote_write": {"url": "http://x/api/v1/write"}},
                sinks=[{"type": "grafana_annotations", "grafana_annotations": {"url": "http://g"}}],
            )
        )


def test_sinks_rejects_duplicate_type() -> None:
    with pytest.raises(ValueError, match="duplicate sink type"):
        Config.model_validate(
            _base_config(
                sinks=[
                    {"type": "remote_write", "remote_write": {"url": "http://a/api/v1/write"}},
                    {"type": "remote_write", "remote_write": {"url": "http://b/api/v1/write"}},
                ]
            )
        )


def test_sinks_rejects_empty_list() -> None:
    with pytest.raises(ValueError, match="at least one sink"):
        Config.model_validate(_base_config(sinks=[]))


class _RecordingSink:
    name = "recording"

    def __init__(self) -> None:
        self.started = False
        self.closed = False
        self.snapshots: list[Any] = []

    async def start(self) -> None:
        self.started = True

    async def emit(self, snapshot: Any) -> None:
        self.snapshots.append(snapshot)

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_application_emits_to_sink_after_successful_run(tmp_path: Path) -> None:
    from promanomaly.main import Application
    from promanomaly.state import GroupSnapshot, Sample

    cfg = Config.model_validate(
        {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://vm"},
            "groups": [
                {"name": "g1", "queries": [{"id": "q", "promql": "up", "detectors": ["MAD"]}]}
            ],
        }
    )
    app = Application(cfg, tmp_path / "c.yaml")
    sink = _RecordingSink()
    app._sinks = [sink]  # type: ignore[list-item]

    # Seed a snapshot and stub the runner so _safe_group_run succeeds.
    snapshot = GroupSnapshot(
        group="g1", timestamp=1.0, samples=[Sample(metric="anomaly_score", labels=(), value=1.0)]
    )
    app._store.write(snapshot)

    class _Result:
        succeeded = True

    async def _fake_run_group(name: str) -> Any:
        return _Result()

    app._runner.run_group = _fake_run_group  # type: ignore[assignment]

    await app._safe_group_run("g1")
    assert sink.snapshots and sink.snapshots[0].group == "g1"


@pytest.mark.asyncio
async def test_emit_to_sinks_isolates_a_failing_sink(tmp_path: Path) -> None:
    """One sink raising must not starve the others, and is counted."""
    from promanomaly.main import Application
    from promanomaly.state import GroupSnapshot, Sample

    app = Application(Config.model_validate(_base_config()), tmp_path / "c.yaml")

    class _BoomSink:
        name = "boom"

        async def start(self) -> None: ...

        async def emit(self, snapshot: Any) -> None:
            raise RuntimeError("kaboom")

        async def aclose(self) -> None: ...

    good = _RecordingSink()
    app._sinks = [_BoomSink(), good]  # type: ignore[list-item]

    snapshot = GroupSnapshot(
        group="g1", timestamp=1.0, samples=[Sample(metric="anomaly_score", labels=(), value=1.0)]
    )
    await app._emit_to_sinks(snapshot)

    # The later sink still received the snapshot despite the earlier one
    # raising, and the failure was counted under the failing sink's name.
    assert good.snapshots and good.snapshots[0].group == "g1"
    metric = app._ops.sink_failures_total.labels(sink="boom", reason="exception")
    assert metric._value.get() == 1.0


@pytest.mark.asyncio
async def test_swap_sinks_rebuilds_on_change_and_noops_when_unchanged(tmp_path: Path) -> None:
    from promanomaly.main import Application

    # Start pull-only; swap in a two-sink list. The old sinks must be
    # closed and the new ones built and started.
    app = Application(Config.model_validate(_base_config()), tmp_path / "c.yaml")
    old = _RecordingSink()
    app._sinks = [old]  # type: ignore[list-item]

    new_cfg = Config.model_validate(
        _base_config(
            sinks=[
                {"type": "remote_write", "remote_write": {"url": "http://x/api/v1/write"}},
                {"type": "grafana_annotations", "grafana_annotations": {"url": "http://g"}},
            ]
        )
    )
    await app._swap_sinks(new_cfg)
    try:
        assert old.closed
        assert [s.name for s in app._sinks] == ["remote_write", "grafana_annotations"]

        # A second swap to a config with identical effective sinks is a
        # no-op — the live sink instances are preserved (not rebuilt).
        app._config = new_cfg
        before = app._sinks
        await app._swap_sinks(new_cfg)
        assert app._sinks is before
    finally:
        for sink in app._sinks:
            await sink.aclose()
