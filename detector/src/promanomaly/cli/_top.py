"""Text rendering for the ``top`` subcommand."""

from __future__ import annotations

from typing import Any

import click


def _format_duration(seconds: float) -> str:
    """Compact ``1h2m`` / ``3m`` / ``45s`` duration for the table."""
    total = int(seconds)
    if total <= 0:
        return "-"
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs and not hours:
        parts.append(f"{secs}s")
    return "".join(parts) or "0s"


def _print_top_text(payload: dict[str, Any]) -> None:
    anomalies = payload.get("anomalies", [])
    if not anomalies:
        click.echo("no firing anomalies")
        return
    header = f"{'SEV':>5}  {'SCORE':>8}  {'DUR':>6}  {'TYPE':<16}  {'GROUP':<16}  ID / LABELS"
    click.echo(header)
    click.echo("-" * len(header))
    for a in anomalies:
        labels = a.get("labels") or {}
        labels_repr = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        detector = a.get("detector", "")
        inst = a.get("detector_instance")
        det_repr = f"{detector}[{inst}]" if inst else detector
        id_repr = a.get("id", "")
        if det_repr:
            id_repr = f"{id_repr} ({det_repr})"
        if labels_repr:
            id_repr = f"{id_repr} {{{labels_repr}}}"
        click.echo(
            f"{a.get('severity', 0.0):>5.2f}  "
            f"{a.get('score', 0.0):>8.3f}  "
            f"{_format_duration(a.get('duration_seconds', 0.0)):>6}  "
            f"{(a.get('type') or '-'):<16}  "
            f"{a.get('group', ''):<16}  "
            f"{id_repr}"
        )


__all__ = ["_print_top_text"]
