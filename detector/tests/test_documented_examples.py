"""CI guard that every shipped example config, every cookbook config
block, and every bundled dashboard stays valid.

- ``examples/configs/*.yaml`` must validate against the pydantic schema.
- Every fenced ```yaml block in ``docs/cookbook/*.md`` that begins with
  ``apiVersion:`` is treated as a complete config and validated, exactly
  like the documented examples the project guide promises CI tests.
- Every ``dashboards/grafana/*.json`` must be well-formed JSON with the
  panels the Grafana importer expects.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from promanomaly.config import load_config

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIGS_DIR = REPO_ROOT / "examples" / "configs"
COOKBOOK_DIR = REPO_ROOT / "docs" / "cookbook"
DASHBOARDS_DIR = REPO_ROOT / "dashboards" / "grafana"

_FENCED_YAML = re.compile(r"```yaml\n(.*?)```", re.DOTALL)


def _example_config_files() -> list[Path]:
    return sorted(CONFIGS_DIR.glob("*.yaml"))


@pytest.mark.parametrize("config_path", _example_config_files(), ids=lambda p: p.name)
def test_example_config_validates(config_path: Path) -> None:
    cfg = load_config(config_path)
    assert cfg.groups, f"{config_path.name} has no groups"


def _cookbook_config_blocks() -> list[tuple[str, str]]:
    blocks: list[tuple[str, str]] = []
    for md in sorted(COOKBOOK_DIR.glob("*.md")):
        for i, match in enumerate(_FENCED_YAML.finditer(md.read_text())):
            body = match.group(1)
            if "apiVersion:" in body:
                blocks.append((f"{md.name}#{i}", body))
    return blocks


@pytest.mark.parametrize(
    "block", _cookbook_config_blocks(), ids=[b[0] for b in _cookbook_config_blocks()]
)
def test_cookbook_config_block_validates(block: tuple[str, str], tmp_path: Path) -> None:
    _name, body = block
    path = tmp_path / "recipe.yaml"
    path.write_text(body)
    cfg = load_config(path)
    assert cfg.groups


def test_cookbook_has_complete_config_blocks() -> None:
    # Guard against a future refactor that strips the testable configs.
    assert len(_cookbook_config_blocks()) >= 8


@pytest.mark.parametrize("dashboard", sorted(DASHBOARDS_DIR.glob("*.json")), ids=lambda p: p.name)
def test_dashboard_is_valid_json(dashboard: Path) -> None:
    data = json.loads(dashboard.read_text())
    assert "title" in data
    assert "panels" in data and isinstance(data["panels"], list)


def test_v11_dashboards_present() -> None:
    names = {p.name for p in DASHBOARDS_DIR.glob("*.json")}
    assert "promanomaly-node-overlay.json" in names
    assert "promanomaly-fleet-density.json" in names
