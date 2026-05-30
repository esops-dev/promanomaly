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
    if secs and not hours and not minutes:
        # Show seconds only when no larger unit is present, keeping the
        # output compact ("1h5m" not "1h5m30s", "45s" not "0h0m45s").
        parts.append(f"{secs}s")
    return "".join(parts) or "0s"


def _print_lint_hints(payload: dict[str, Any]) -> None:
    hints = payload.get("metadata_lint") or []
    if not hints:
        return
    click.echo("metadata lint:")
    for hint in hints:
        target = f"{hint.get('group', '')}/{hint.get('query', '')}"
        click.echo(f"  ! {target}: {hint.get('message', '')}")
    click.echo("")


def _print_blast_radius(payload: dict[str, Any]) -> None:
    rows = payload.get("blast_radius") or []
    if not rows:
        return
    click.echo("")
    click.echo("blast radius:")
    header = (
        f"  {'SCOPE':<8}  {'GROUP':<16}  {'FIRING':>7}"
        f"  {'TOTAL':>7}  {'FRAC':>6}  {'MAX DUR':>7}  COHORT"
    )
    click.echo(header)
    for r in rows:
        cohort = ""
        if r.get("cohort_label"):
            key_repr = ",".join(f"{k}={v}" for k, v in sorted((r.get("cohort_key") or {}).items()))
            cohort = f"{r['cohort_label']} {{{key_repr}}}" if key_repr else r["cohort_label"]
        click.echo(
            f"  {r.get('scope', ''):<8}  "
            f"{r.get('group', ''):<16}  "
            f"{r.get('firing', 0):>7}  "
            f"{r.get('total', 0):>7}  "
            f"{r.get('fraction', 0.0):>5.0%}  "
            f"{_format_duration(r.get('max_co_firing_seconds', 0.0)):>7}  "
            f"{cohort}"
        )


def _print_top_text(payload: dict[str, Any]) -> None:
    _print_lint_hints(payload)
    anomalies = payload.get("anomalies", [])
    if not anomalies:
        click.echo("no firing anomalies")
        _print_blast_radius(payload)
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
    _print_blast_radius(payload)


__all__ = ["_print_top_text"]
