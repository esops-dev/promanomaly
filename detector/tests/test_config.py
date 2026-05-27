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
    # ``w`` (weeks) added in v0.5 so stratified detectors with multi-week
    # lookbacks (DayOfWeekMAD's ``lookback: 4w``) read naturally.
    assert parse_duration("4w") == 4 * 7 * 86400.0
    assert parse_duration("1w") == 7 * 86400.0


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


def test_ensemble_voting_rejects_unreachable_min_detectors() -> None:
    """voting + min_detectors > every query's detector list is a silent
    never-fire footgun. The validator should reject the config so the
    user sees the typo at apply time."""
    raw = {
        **MIN_CONFIG,
        "groups": [
            {
                "name": "g1",
                "ensemble": {"method": "voting", "min_detectors": 5},
                "queries": [
                    {
                        "id": "m",
                        "promql": "up",
                        "detectors": [
                            {"name": "MAD"},
                            {"name": "Hampel"},
                        ],
                    }
                ],
            }
        ],
    }
    with pytest.raises(ValueError, match=r"ensemble\.min_detectors=5"):
        Config.model_validate(raw)


def test_ensemble_voting_allows_mixed_queries() -> None:
    """voting + min_detectors reachable by at least one query is fine,
    even when other queries in the same group have fewer detectors
    (the operator may want a partial composite signal)."""
    raw = {
        **MIN_CONFIG,
        "groups": [
            {
                "name": "g1",
                "ensemble": {"method": "voting", "min_detectors": 3},
                "queries": [
                    {
                        "id": "m_big",
                        "promql": "up",
                        "detectors": [
                            {"name": "MAD"},
                            {"name": "Hampel"},
                            {"name": "IQR"},
                        ],
                    },
                    {
                        "id": "m_small",
                        "promql": "up{job='x'}",
                        "detectors": [{"name": "MAD"}],
                    },
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    assert cfg.groups[0].ensemble is not None
    assert cfg.groups[0].ensemble.min_detectors == 3


def test_ensemble_max_method_does_not_trip_reachability_check() -> None:
    """``max`` has no min_detectors semantics — any non-empty detector
    list fires it. The validator must not block ``max`` configs."""
    raw = {
        **MIN_CONFIG,
        "groups": [
            {
                "name": "g1",
                "ensemble": {"method": "max", "min_detectors": 99},
                "queries": [
                    {
                        "id": "m",
                        "promql": "up",
                        "detectors": [{"name": "MAD"}],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    assert cfg.groups[0].ensemble is not None


def test_discovery_forget_after_runs_must_outlast_grace() -> None:
    """Audit guard: an operator who sets forget_after_runs below a
    query's expect_grace_runs would silently hide signal_missing —
    the transition would fire and the entry would get dropped before
    the next scrape. Reject the combination at config-load time."""
    raw = {
        **MIN_CONFIG,
        "safety": {"discovery": {"forget_after_runs": 2}},
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "cpu_{{ instance }}",
                        "promql": 'up{instance="{{ instance }}"}',
                        "expect_grace_runs": 5,
                        "discover": [{"variable": "instance", "probe": "up", "label": "instance"}],
                        "detectors": [{"name": "MAD"}],
                    }
                ],
            }
        ],
    }
    with pytest.raises(Exception, match="forget_after_runs"):
        Config.model_validate(raw)


def test_discovery_forget_after_runs_above_grace_is_ok() -> None:
    raw = {
        **MIN_CONFIG,
        "safety": {"discovery": {"forget_after_runs": 50}},
        "groups": [
            {
                "name": "g1",
                "queries": [
                    {
                        "id": "cpu_{{ instance }}",
                        "promql": 'up{instance="{{ instance }}"}',
                        "expect_grace_runs": 5,
                        "discover": [{"variable": "instance", "probe": "up", "label": "instance"}],
                        "detectors": [{"name": "MAD"}],
                    }
                ],
            }
        ],
    }
    cfg = Config.model_validate(raw)
    assert cfg.safety.discovery.forget_after_runs == 50


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
