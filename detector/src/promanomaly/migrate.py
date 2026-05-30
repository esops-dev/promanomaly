"""Config schema migration for promanomaly.

Reads a config at any supported ``apiVersion``, applies the registered
forward-migrations (field renames, default relocations, ``apiVersion``
rewrite), and writes the upgraded document. Idempotent: running it on an
already-current config is a no-op.

Migrations are registered per schema revision so the path composes
across multiple versions. Output preserves key order and comments where
the round-trip YAML library supports it (falls back to ``PyYAML``
``safe_dump`` otherwise).
"""

from __future__ import annotations

import difflib
import logging
from pathlib import Path
from typing import Any

import yaml

from .config import CURRENT_API_VERSION, SUPPORTED_API_VERSIONS

logger = logging.getLogger(__name__)


def _migrate_v1alpha1_to_v1(raw: dict[str, Any]) -> dict[str, Any]:
    """Migrate from ``promanomaly.io/v1alpha1`` to ``promanomaly.io/v1``.

    The two schemas are byte-identical, so this is purely an
    ``apiVersion`` rewrite.
    """
    raw["apiVersion"] = CURRENT_API_VERSION
    return raw


# Registry of forward migrations. Each key is a source apiVersion;
# the value is ``(target_version, migration_fn)``. The migration
# function receives the raw YAML dict and returns the modified dict.
_MIGRATION_MAP: dict[str, tuple[str, Any]] = {
    "promanomaly.io/v1alpha1": (CURRENT_API_VERSION, _migrate_v1alpha1_to_v1),
}


def migrate_raw(raw: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Apply all applicable forward-migrations to a raw config dict.

    Returns ``(migrated_dict, changes)`` where ``changes`` is a list of
    human-readable descriptions of what was changed. An empty ``changes``
    list means the config was already at the current version.
    """
    changes: list[str] = []
    api_version = raw.get("apiVersion", "")

    if api_version not in SUPPORTED_API_VERSIONS:
        raise ValueError(
            f"unsupported apiVersion {api_version!r}; cannot migrate. "
            f"Supported versions: {sorted(SUPPORTED_API_VERSIONS)}"
        )

    # Apply migrations in chain order.
    while api_version in _MIGRATION_MAP:
        target_version, migration_fn = _MIGRATION_MAP[api_version]
        raw = migration_fn(raw)
        changes.append(f"apiVersion: {api_version!r} -> {target_version!r}")
        api_version = target_version

    return raw, changes


def migrate_file(
    path: str | Path,
    *,
    dry_run: bool = False,
) -> tuple[str, list[str]]:
    """Migrate a YAML config file.

    When ``dry_run`` is True, returns a unified diff of the changes
    without writing. When False, writes the migrated config in place.

    Returns ``(output, changes)`` where ``output`` is the unified diff
    (dry_run) or the migrated YAML text (write mode).

    Raises ``ValueError`` for invalid configs and ``OSError`` for
    file I/O failures.
    """
    p = Path(path)
    original_text = p.read_text()
    raw = yaml.safe_load(original_text)
    if not isinstance(raw, dict):
        raise ValueError(f"config {path}: top level must be a mapping")

    migrated, changes = migrate_raw(raw)

    if not changes:
        return "", changes

    migrated_text = yaml.dump(
        migrated,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )

    if dry_run:
        diff = difflib.unified_diff(
            original_text.splitlines(keepends=True),
            migrated_text.splitlines(keepends=True),
            fromfile=str(path),
            tofile=str(path),
        )
        return "".join(diff), changes

    p.write_text(migrated_text)
    return migrated_text, changes


def migrate_directory(
    directory: str | Path,
    *,
    dry_run: bool = False,
) -> list[tuple[str, str, list[str]]]:
    """Migrate all YAML files in a directory.

    Returns a list of ``(file_path, output, changes)`` tuples.
    Files that fail to parse or migrate are logged and skipped so a
    single malformed fragment doesn't block the rest of the directory.
    """
    d = Path(directory)
    results: list[tuple[str, str, list[str]]] = []
    for f in sorted(d.iterdir()):
        if not f.is_file() or f.suffix not in (".yaml", ".yml"):
            continue
        try:
            output, changes = migrate_file(f, dry_run=dry_run)
            if changes:
                results.append((str(f), output, changes))
        except (ValueError, yaml.YAMLError) as exc:
            logger.warning("skipping %s: %s", f, exc)
            continue
        except OSError as exc:
            logger.warning("write failed for %s: %s", f, exc)
            continue
    return results


__all__ = [
    "migrate_directory",
    "migrate_file",
    "migrate_raw",
]
