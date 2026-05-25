"""Config-schema validation tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from promanomaly.config import (
    CURRENT_API_VERSION,
    Config,
    load_config,
    parse_duration,
)

MIN_CONFIG: dict[str, object] = {
    "apiVersion": CURRENT_API_VERSION,
    "datasource": {"url": "http://vm:8428/"},
    "groups": [
        {
            "name": "g1",
            "queries": [
                {
                    "id": "metric_a",
                    "promql": "up",
                    "detectors": [{"name": "MAD"}],
                }
            ],
        }
    ],
}


def test_parse_duration_supports_human_units() -> None:
    assert parse_duration("30s") == 30.0
    assert parse_duration("5m") == 300.0
    assert parse_duration("1h") == 3600.0
    assert parse_duration("1.5h") == 5400.0
    assert parse_duration("250ms") == 0.25
    assert parse_duration(60) == 60.0


def test_minimum_config_validates() -> None:
    cfg = Config.model_validate(MIN_CONFIG)
    assert cfg.groups[0].name == "g1"
    assert cfg.defaults.window_seconds == 3600.0
    assert cfg.defaults.warmup_policy == "emit_warming_up"
    assert cfg.server.refresh_interval_seconds == 60.0


def test_unknown_api_version_rejected() -> None:
    bad = {**MIN_CONFIG, "apiVersion": "promanomaly.io/v999"}
    with pytest.raises(Exception, match="unsupported apiVersion"):
        Config.model_validate(bad)


@pytest.mark.parametrize("bad_id", ["with:colon", "spaces here", "1leading_digit", ""])
def test_invalid_query_id_rejected(bad_id: str) -> None:
    from pydantic import ValidationError

    cfg = {**MIN_CONFIG}
    cfg["groups"] = [
        {
            "name": "g1",
            "queries": [{"id": bad_id, "promql": "up", "detectors": [{"name": "MAD"}]}],
        }
    ]
    with pytest.raises(ValidationError):
        Config.model_validate(cfg)


def test_duplicate_group_names_rejected() -> None:
    cfg = {
        **MIN_CONFIG,
        "groups": [
            MIN_CONFIG["groups"][0],  # type: ignore[index]
            MIN_CONFIG["groups"][0],  # type: ignore[index]
        ],
    }
    with pytest.raises(Exception, match="duplicate group name"):
        Config.model_validate(cfg)


def test_duplicate_detector_without_instance_rejected() -> None:
    cfg = {
        **MIN_CONFIG,
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "m1",
                        "promql": "up",
                        "detectors": [
                            {"name": "MAD", "params": {"window": "5m"}},
                            {"name": "MAD", "params": {"window": "1h"}},
                        ],
                    }
                ],
            }
        ],
    }
    with pytest.raises(Exception, match="must declare a unique 'instance:'"):
        Config.model_validate(cfg)


def test_duplicate_detector_with_distinct_instance_accepted() -> None:
    cfg = {
        **MIN_CONFIG,
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "m1",
                        "promql": "up",
                        "detectors": [
                            {"name": "MAD", "instance": "short"},
                            {"name": "MAD", "instance": "long"},
                        ],
                    }
                ],
            }
        ],
    }
    parsed = Config.model_validate(cfg)
    detectors = parsed.groups[0].queries[0].detectors
    assert [d.instance for d in detectors] == ["short", "long"]


def test_aggressive_refresh_flag() -> None:
    # window 1h, refresh 30s → 30 < 600 → aggressive.
    aggressive = {
        **MIN_CONFIG,
        "server": {"refresh_interval": "30s"},
        "defaults": {"window": "1h", "step": "15s", "min_points": 60},
    }
    assert Config.model_validate(aggressive).aggressive_refresh is True

    # window 1h, refresh 15m → 900 > 600 → not aggressive.
    safe = {
        **MIN_CONFIG,
        "server": {"refresh_interval": "15m"},
        "defaults": {"window": "1h", "step": "15s", "min_points": 60},
    }
    assert Config.model_validate(safe).aggressive_refresh is False


def test_load_config_reads_dev_yaml(tmp_path: Path) -> None:
    dev = Path(__file__).resolve().parents[2] / "docker" / "dev-config.yaml"
    cfg = load_config(dev)
    assert cfg.apiVersion == CURRENT_API_VERSION
    assert cfg.groups[0].name == "node_dev"
    assert cfg.defaults.warmup_policy == "emit_warming_up"
    assert cfg.safety.on_source_failure == "serve_stale"
    # smoke: re-serialise round-trips
    raw = cfg.model_dump(mode="json")
    assert raw["apiVersion"] == CURRENT_API_VERSION
    del tmp_path  # unused
