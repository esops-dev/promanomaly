"""CLI smoke tests via Click's CliRunner."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from promanomaly.cli import cli
from promanomaly.config import CURRENT_API_VERSION


def _write_config(tmp_path: Path) -> Path:
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "groups": [
            {
                "name": "g",
                "queries": [{"id": "m", "promql": "up", "detectors": [{"name": "MAD"}]}],
            }
        ],
    }
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def test_validate_ok(tmp_path: Path) -> None:
    path = _write_config(tmp_path)
    result = CliRunner().invoke(cli, ["validate", "--config", str(path)])
    assert result.exit_code == 0
    assert "OK:" in result.output
    assert CURRENT_API_VERSION in result.output


def test_validate_bad_yaml_exits_nonzero(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("apiVersion: promanomaly.io/v999\nfoo: bar\n")
    result = CliRunner().invoke(cli, ["validate", "--config", str(bad)])
    assert result.exit_code != 0
    assert "INVALID:" in result.output


def test_validate_missing_file_exits_nonzero(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, ["validate", "--config", str(tmp_path / "nope.yaml")])
    assert result.exit_code != 0


def test_detectors_list_local_returns_built_ins() -> None:
    result = CliRunner().invoke(cli, ["detectors", "list"])
    assert result.exit_code == 0
    rows = json.loads(result.output)
    names = {row["name"] for row in rows}
    assert {"MAD", "Hampel"}.issubset(names)
    hampel = next(r for r in rows if r["name"] == "Hampel")
    assert hampel["defaults"] == {"t0": 3}


def test_no_subcommand_requires_config() -> None:
    result = CliRunner().invoke(cli, [])
    assert result.exit_code != 0
    assert "--config is required" in result.output
