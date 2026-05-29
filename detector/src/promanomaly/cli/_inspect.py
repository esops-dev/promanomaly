"""Text-rendering helpers for the ``inspect`` subcommand."""

from __future__ import annotations

from typing import Any

import click


def _print_inspect_text(payload: dict[str, Any]) -> None:
    if payload.get("status") == "error":
        click.echo(f"ERROR: {payload.get('reason')} — {payload.get('detail')}")
        return
    click.echo(f"id     : {payload['id']}")
    click.echo(f"group  : {payload['group']}")
    click.echo(f"query  : {payload['query']}")
    sel = payload.get("label_selector") or {}
    sel_repr = ",".join(f"{k}={v}" for k, v in sorted(sel.items())) if sel else "(none)"
    click.echo(f"labels : {sel_repr}")
    click.echo(f"matched: {payload['matched_series']} / {payload['total_series']} series")
    for hint in payload.get("metadata_lint", []):
        click.echo(f"lint   : {hint.get('message', '')}")
    for series in payload.get("series", []):
        labels_repr = ",".join(f"{k}={v}" for k, v in sorted(series["labels"].items())) or "-"
        click.echo("")
        click.echo(f"  series       : {labels_repr}")
        click.echo(f"  samples      : {series['sample_count']}")
        click.echo(f"  latest_y     : {series['latest_y']}")
        click.echo(
            f"  warming_up   : {series['warming_up']} ({series['warmup_observed_runs']} runs)"
        )
        for det in series.get("detectors", []):
            inst = f"[{det['instance']}]" if det.get("instance") else ""
            click.echo(f"  detector     : {det['name']}{inst} ({det['status']})")
            if det["status"] == "ok":
                click.echo(
                    f"    score      : {det['score']:.4f}  "
                    f"baseline={det['baseline']:.4f}  "
                    f"threshold={det['threshold']}  "
                    f"outside={det['is_outside'] or det.get('statistical_outside', False)}"
                )
            if det.get("calibration"):
                cal = det["calibration"]
                click.echo(
                    f"    confidence : {cal['confidence_score']:.3f}  "
                    f"stability={cal['baseline_stability']:.3f}  "
                    f"f1={cal['f1']:.3f}"
                )
