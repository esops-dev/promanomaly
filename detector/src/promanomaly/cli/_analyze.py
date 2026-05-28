"""Pure helpers for the ``analyze`` subcommand.

Kept out of :mod:`cli.__init__` so the offline change-point routine
stays unit-testable without pulling in Click context or the live
PromQLSource.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

import click

from ..config import parse_timestamp
from ..source import QuerySeries

# Backwards-compatible name kept for tests that import the original
# helper. New code should use :func:`promanomaly.config.parse_timestamp`.
_parse_analyze_timestamp = parse_timestamp


def _run_offline_change_points(
    *,
    series_list: list[QuerySeries],
    detector_name: str,
    model: str,
    penalty: float,
    top: int,
) -> list[dict[str, Any]]:
    """Run the chosen ruptures algorithm and return ranked findings.

    Imported lazily so users invoking unrelated CLI subcommands don't pay
    the scipy/ruptures import cost on every invocation. ``ruptures`` is
    an optional dependency surfaced via the ``[analyze]`` extra so the
    runtime container can omit it; install with
    ``pip install promanomaly[analyze]`` to enable the CLI.
    """
    import numpy as np

    try:
        import ruptures as rpt
    except ImportError as exc:
        raise click.ClickException(
            "promanomaly analyze requires the 'ruptures' library — "
            "install with: pip install 'promanomaly[analyze]'"
        ) from exc

    findings: list[dict[str, Any]] = []
    for series in series_list:
        timestamps = series.samples["timestamp"].to_numpy(dtype=float, copy=False)
        values = series.samples["y"].to_numpy(dtype=float, copy=False)
        if values.size < 4:
            # Skip silently — too short to find anything useful and
            # ruptures will raise on tiny inputs.
            continue
        signal = values.reshape(-1, 1)
        breakpoints: list[int]
        if detector_name in ("ruptures", "pelt"):
            algo = rpt.Pelt(model=model).fit(signal)
            breakpoints = algo.predict(pen=penalty)
        elif detector_name == "binseg":
            algo = rpt.Binseg(model=model).fit(signal)
            # Binseg.predict takes n_bkps OR pen — use pen for parity
            # with Pelt so the same --penalty knob behaves consistently.
            breakpoints = algo.predict(pen=penalty)
        elif detector_name == "window":
            # Window-based change-point detection; uses a fixed window
            # width sized as a fraction of the signal length.
            width = max(10, values.size // 20)
            algo = rpt.Window(width=width, model=model).fit(signal)
            breakpoints = algo.predict(pen=penalty)
        else:
            raise click.UsageError(f"unknown detector {detector_name!r}")

        # ruptures appends the end of the signal as the final breakpoint
        # — drop it so the output only carries genuine change-points.
        breakpoints = [b for b in breakpoints if b < values.size]
        for bp in breakpoints:
            # Score: absolute mean shift across the break, normalised by
            # the in-segment standard deviation. Approximate but
            # interpretable; lets the caller rank breaks by magnitude.
            left = values[max(0, bp - 30) : bp]
            right = values[bp : min(values.size, bp + 30)]
            if left.size == 0 or right.size == 0:
                score = 0.0
            else:
                pooled_std = float(np.std(np.concatenate([left, right])))
                pooled_std = max(pooled_std, 1e-12)
                score = abs(float(np.mean(right)) - float(np.mean(left))) / pooled_std
            findings.append(
                {
                    "labels": series.labels,
                    "timestamp": float(timestamps[bp]),
                    "iso8601": _format_iso(float(timestamps[bp])),
                    "score": float(score),
                    "index": int(bp),
                    "left_mean": float(left.mean()) if left.size else None,
                    "right_mean": float(right.mean()) if right.size else None,
                }
            )

    findings.sort(key=lambda r: r["score"], reverse=True)
    return findings[:top]


def _format_iso(ts: float) -> str:
    return _dt.datetime.fromtimestamp(ts, tz=_dt.UTC).isoformat()


def _print_analyze_text(findings: list[dict[str, Any]]) -> None:
    if not findings:
        click.echo("no change-points detected")
        return
    click.echo(f"{'when':30}  {'score':>8}  labels")
    for row in findings:
        labels = ",".join(f"{k}={v}" for k, v in sorted(row["labels"].items())) or "-"
        click.echo(f"{row['iso8601']:30}  {row['score']:8.2f}  {labels}")
