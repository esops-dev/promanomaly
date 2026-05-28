"""Generic CLI helpers shared across subcommands."""

from __future__ import annotations

from typing import Any


def _coerce_param(value: str) -> Any:
    """Best-effort scalar coercion for CLI ``--param key=value`` arguments."""
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    for cast in (int, float):
        try:
            return cast(value)
        except ValueError:
            continue
    return value


def _safe_float(value: Any) -> Any:
    """Coerce a DataFrame.to_dict() value into something json.dumps accepts.

    NumPy scalars come back as ``numpy.float64`` / ``numpy.bool_``; the
    ``isinstance`` check covers Python primitives and lets ``json``
    handle NumPy scalars via their ``__float__`` / ``__bool__`` hooks.
    """
    if isinstance(value, float | int | bool | str) or value is None:
        return value
    return str(value)
