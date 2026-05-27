"""Backtesting with synthetic-anomaly injection over real history.

Operators tuning detector parameters need precision / recall / F1 on
*production-shaped* signals — clean synthetic series only validate
behaviour against textbook anomalies, while real history exposes the
fat tails, daily cycles, and missing-data gaps detectors actually
encounter.

This module exposes two entry points (both invoked from the Makefile):

- ``promanomaly backtest --group <g>`` — pulls a recent window of real
  history for every query in the named group and scores it against
  every configured detector. Reports per-detector firing rates so
  operators can spot detectors that false-positive on production data.
- ``promanomaly backtest --group <g> --inject`` — same as above, but
  also walks the history with a sliding window and injects each of the
  built-in anomaly patterns (point spike, step change, level drift) at
  known offsets. The runner sees only the (clean | injected) window;
  each detector's verdict becomes a true positive / false positive /
  true negative / false negative and feeds standard P / R / F1.

Both forms emit either JSON (machine-readable, suitable for CI gates)
or Markdown (suitable for paste-into-PR-description). Pre-deploy
workflow: ``make backtest-inject GROUP=<g>`` on a representative
production window before merging a detector-tuning change.
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as _dt
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import click
import numpy as np
import pandas as pd

from .calibration import INJECTION_PATTERNS
from .config import Config, QueryConfig, load_config, parse_duration
from .detectors import UnknownDetectorError, get
from .runner import _build_detector
from .source import PromQLSource, QuerySeries, SourceQueryError

# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class DetectorMetrics:
    """Per-detector precision / recall / F1 over the backtest run.

    ``magnitude_breakdown`` is per-pattern (``point_spike``,
    ``step_change``, ``level_drift``) — the operator-facing tuning
    knob: "we false-negative on drifts but catch spikes; raise t0".
    """

    detector: str
    instance: str | None = None
    true_positive: int = 0
    false_positive: int = 0
    true_negative: int = 0
    false_negative: int = 0
    fires_on_real: int = 0
    real_windows: int = 0
    magnitude_breakdown: dict[str, dict[str, int]] = field(default_factory=dict)

    @property
    def precision(self) -> float:
        denom = self.true_positive + self.false_positive
        return self.true_positive / denom if denom else 0.0

    @property
    def recall(self) -> float:
        denom = self.true_positive + self.false_negative
        return self.true_positive / denom if denom else 0.0

    @property
    def f1(self) -> float:
        precision, recall = self.precision, self.recall
        denom = precision + recall
        return 2.0 * precision * recall / denom if denom else 0.0

    @property
    def real_fire_rate(self) -> float:
        return self.fires_on_real / self.real_windows if self.real_windows else 0.0

    def as_dict(self) -> dict[str, Any]:
        out = dataclasses.asdict(self)
        out["precision"] = self.precision
        out["recall"] = self.recall
        out["f1"] = self.f1
        out["real_fire_rate"] = self.real_fire_rate
        return out


@dataclass
class QueryReport:
    query_id: str
    promql: str
    series_count: int = 0
    detectors: list[DetectorMetrics] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "promql": self.promql,
            "series_count": self.series_count,
            "detectors": [d.as_dict() for d in self.detectors],
        }


@dataclass
class BacktestReport:
    group: str
    inject: bool
    window_seconds: float
    step_seconds: float
    end: float
    queries: list[QueryReport] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "group": self.group,
            "inject": self.inject,
            "window_seconds": self.window_seconds,
            "step_seconds": self.step_seconds,
            "end": self.end,
            "queries": [q.as_dict() for q in self.queries],
        }


# ---------------------------------------------------------------------------
# Detection helper
# ---------------------------------------------------------------------------


def _detector_fires(
    cls: type, params: dict[str, Any], window: pd.DataFrame, threshold: float
) -> bool:
    """Mirror the runner's verdict logic for one detector run.

    A detector that knows its own threshold semantics (BOCPD's
    posterior probability) sets ``is_outside=True`` directly; everyone
    else relies on the raw score crossing the alert threshold.
    """
    try:
        detector = _build_detector(cls, params)
        row = detector.fit_score(window, "", params).iloc[-1]
    except Exception:
        return False
    if bool(row.get("is_outside", False)):
        return True
    return float(row.get("score", 0.0)) >= threshold


def _parse_timestamp(value: str, *, now: float) -> float:
    """Parse RFC3339 / unix-seconds / 'now' / relative '-7d' to unix seconds.

    Mirrors the ``analyze`` CLI's parser so backtest and analyze share
    one mental model for time inputs.
    """
    value = value.strip()
    if value == "now":
        return float(now)
    if value.startswith("-"):
        return float(now) - parse_duration(value[1:])
    try:
        return float(value)
    except ValueError:
        pass
    try:
        dt = _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            f"invalid timestamp {value!r}; expected RFC3339, unix seconds, 'now', or '-7d'"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.UTC)
    return dt.timestamp()


def _effective_threshold(params: dict[str, Any], fallback: float) -> float:
    raw = params.get("threshold")
    if raw is None:
        return fallback
    try:
        return float(raw)
    except (TypeError, ValueError):
        return fallback


# ---------------------------------------------------------------------------
# Backtest core
# ---------------------------------------------------------------------------


def _build_plans(query: QueryConfig, config: Config) -> list[dict[str, Any]]:
    """Validate detectors + merge defaults, mirroring Runner._fetch_and_plan."""
    plans: list[dict[str, Any]] = []
    defaults = config.defaults
    for entry in query.detectors:
        try:
            cls = get(entry.name)
        except UnknownDetectorError as exc:
            raise click.ClickException(str(exc)) from exc
        params: dict[str, Any] = {}
        params.update(getattr(cls, "defaults", {}) or {})
        params.update(entry.params or {})
        params.setdefault("window", defaults.window)
        params.setdefault("step", defaults.step)
        plans.append(
            {
                "name": entry.name,
                "instance": entry.instance,
                "cls": cls,
                "params": params,
            }
        )
    return plans


def _sliding_windows(samples: pd.DataFrame, window_samples: int, stride: int) -> list[pd.DataFrame]:
    """Carve a long history into overlapping rolling windows.

    Each window matches the shape the detector would see at one tick of
    its scheduled cadence. We bound the number of windows so a 7-day
    history at 15s step doesn't yield 40k sub-windows.
    """
    if len(samples) <= window_samples:
        return [samples.reset_index(drop=True)]
    windows: list[pd.DataFrame] = []
    for start in range(0, len(samples) - window_samples + 1, stride):
        windows.append(samples.iloc[start : start + window_samples].reset_index(drop=True))
    return windows


def _inject(values: np.ndarray, pattern: str) -> np.ndarray:
    fn = INJECTION_PATTERNS[pattern]
    return fn(values)  # type: ignore[no-any-return]


def _build_injected_window(window: pd.DataFrame, pattern: str) -> pd.DataFrame:
    values = window["y"].to_numpy(dtype=float, copy=False)
    injected = _inject(values, pattern)
    return pd.DataFrame({"timestamp": window["timestamp"].to_numpy(copy=False), "y": injected})


# Default cap to keep runtime predictable. Each window pays one detector
# pass per pattern (3) + 1 clean — 4 passes per detector per window. 200
# windows by N detectors by 4 = bounded for "make backtest" use.
_DEFAULT_MAX_WINDOWS: int = 200


def _evaluate_series(
    *,
    series: QuerySeries,
    plans: list[dict[str, Any]],
    window_samples: int,
    inject: bool,
    metrics_by_key: dict[tuple[str, str | None], DetectorMetrics],
    max_windows: int,
) -> None:
    """Walk one series's sliding windows and accumulate per-detector metrics.

    Each plan carries its own ``_threshold`` (resolved by the caller via
    :func:`_effective_threshold`) so per-detector overrides like BOCPD's
    probability cutoff are honoured uniformly with the live runner. A
    single shared ``threshold`` arg here would collapse them onto the
    group-level alert score and break BOCPD's F1.
    """
    stride = max(1, window_samples // 4)
    windows = _sliding_windows(series.samples, window_samples, stride)
    if len(windows) > max_windows:
        windows = windows[:max_windows]
    for window in windows:
        for plan in plans:
            key = (plan["name"], plan["instance"])
            metrics = metrics_by_key.setdefault(
                key, DetectorMetrics(detector=plan["name"], instance=plan["instance"])
            )
            threshold = plan["_threshold"]
            real_fire = _detector_fires(plan["cls"], plan["params"], window, threshold)
            metrics.real_windows += 1
            if real_fire:
                metrics.fires_on_real += 1
            if not inject:
                continue
            # Inject every pattern on top of THIS window. A detector
            # that fires on the unmodified window counts as a false
            # positive against the clean control. Detectors that fire
            # on the injected windows count as true positives; misses
            # are false negatives.
            if real_fire:
                metrics.false_positive += 1
            else:
                metrics.true_negative += 1
            for pattern in INJECTION_PATTERNS:
                injected_window = _build_injected_window(window, pattern)
                fired = _detector_fires(plan["cls"], plan["params"], injected_window, threshold)
                bucket = metrics.magnitude_breakdown.setdefault(
                    pattern, {"true_positive": 0, "false_negative": 0}
                )
                if fired:
                    metrics.true_positive += 1
                    bucket["true_positive"] += 1
                else:
                    metrics.false_negative += 1
                    bucket["false_negative"] += 1


async def run_backtest(
    *,
    config: Config,
    source: PromQLSource,
    group_name: str,
    inject: bool = False,
    end: float | None = None,
    start: float | None = None,
    max_windows: int = _DEFAULT_MAX_WINDOWS,
) -> BacktestReport:
    """Pull history for ``group_name`` and score each detector.

    ``end`` defaults to ``now``. When ``start`` is supplied the
    history range is ``[start, end]`` exactly; otherwise the lookback
    defaults to the group's ``defaults.window`` (1x for real-history
    mode, 4x for ``--inject`` mode so the sliding-window backtest has
    multiple distinct sub-windows to score).
    """
    group = next((g for g in config.groups if g.name == group_name), None)
    if group is None:
        raise click.ClickException(f"unknown group {group_name!r}")

    defaults = config.defaults
    end_ts = end if end is not None else time.time()
    base_window = defaults.window_seconds
    step_seconds = defaults.step_seconds
    if start is not None:
        if start >= end_ts:
            raise click.ClickException("--from must be before --to")
        history_seconds = end_ts - start
    else:
        # In inject mode we pull a fat history so sliding-window injection
        # has multiple distinct windows to evaluate. In bare backtest mode
        # the single window is enough.
        history_seconds = base_window * 4.0 if inject else base_window
    window_samples = max(1, int(base_window / max(1.0, step_seconds)))

    report = BacktestReport(
        group=group_name,
        inject=inject,
        window_seconds=base_window,
        step_seconds=step_seconds,
        end=end_ts,
    )

    for query in group.queries:
        if query.discover:
            # Discovery-expanded queries need probe + render before we
            # know the actual PromQL to backtest. That's a future
            # enhancement; for now skip and warn so the report is
            # explicit about what was and was not scored.
            report.queries.append(
                QueryReport(
                    query_id=query.id,
                    promql=query.promql,
                    series_count=0,
                    detectors=[],
                )
            )
            continue
        plans = _build_plans(query, config)
        threshold = (query.alert_thresholds or defaults.alert_thresholds).score
        # Resolve each plan's effective threshold once per query — BOCPD
        # and other detectors that handle their own threshold semantics
        # opt in by setting ``threshold`` in params; everyone else falls
        # back to the query/group alert score. The runner uses the same
        # _effective_threshold helper at scoring time, so backtest and
        # live runs agree on the cutoff.
        for plan in plans:
            plan["_threshold"] = _effective_threshold(plan["params"], threshold)
        try:
            qresult = await source.range_query(
                promql=query.promql,
                end=end_ts,
                window_seconds=history_seconds,
                step_seconds=step_seconds,
                max_series=config.safety.max_series_per_query,
            )
        except SourceQueryError as exc:
            click.echo(
                f"backtest_query_failed group={group_name} query={query.id} reason={exc.reason}",
                err=True,
            )
            report.queries.append(
                QueryReport(query_id=query.id, promql=query.promql, series_count=0)
            )
            continue

        metrics_by_key: dict[tuple[str, str | None], DetectorMetrics] = {}
        for series in qresult.series:
            _evaluate_series(
                series=series,
                plans=plans,
                window_samples=window_samples,
                inject=inject,
                metrics_by_key=metrics_by_key,
                max_windows=max_windows,
            )

        report.queries.append(
            QueryReport(
                query_id=query.id,
                promql=query.promql,
                series_count=len(qresult.series),
                detectors=list(metrics_by_key.values()),
            )
        )
    return report


# ---------------------------------------------------------------------------
# Output rendering
# ---------------------------------------------------------------------------


def render_markdown(report: BacktestReport) -> str:
    """Render the report as a Markdown summary suitable for PR comments."""
    lines: list[str] = []
    lines.append(f"# Backtest report — group `{report.group}`")
    lines.append("")
    mode = "with synthetic-anomaly injection" if report.inject else "real-history fire-rate"
    lines.append(f"Mode: **{mode}**")
    lines.append(f"Window: {report.window_seconds:.0f}s @ step {report.step_seconds:.0f}s")
    lines.append("")
    for query in report.queries:
        lines.append(f"## Query `{query.query_id}`")
        lines.append("")
        lines.append(f"`{query.promql}` — {query.series_count} series")
        lines.append("")
        if not query.detectors:
            lines.append("_no data (query empty, errored, or discovery-templated)_")
            lines.append("")
            continue
        if report.inject:
            lines.append(
                "| Detector | Instance | Precision | Recall | F1 | "
                "TP | FP | FN | TN | Real fire-rate |"
            )
            lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
            for d in query.detectors:
                lines.append(
                    f"| {d.detector} | {d.instance or '-'} | "
                    f"{d.precision:.2f} | {d.recall:.2f} | {d.f1:.2f} | "
                    f"{d.true_positive} | {d.false_positive} | "
                    f"{d.false_negative} | {d.true_negative} | "
                    f"{d.real_fire_rate:.2%} |"
                )
            lines.append("")
            for d in query.detectors:
                if not d.magnitude_breakdown:
                    continue
                suffix = f" [{d.instance}]" if d.instance else ""
                lines.append(f"_{d.detector}_{suffix} per pattern:")
                lines.append("")
                lines.append("| Pattern | TP | FN | Recall |")
                lines.append("|---|---:|---:|---:|")
                for pattern, counts in sorted(d.magnitude_breakdown.items()):
                    tp = counts.get("true_positive", 0)
                    fn = counts.get("false_negative", 0)
                    recall = tp / (tp + fn) if (tp + fn) else 0.0
                    lines.append(f"| {pattern} | {tp} | {fn} | {recall:.2f} |")
                lines.append("")
        else:
            lines.append("| Detector | Instance | Windows | Fires on real history | Rate |")
            lines.append("|---|---|---:|---:|---:|")
            for d in query.detectors:
                lines.append(
                    f"| {d.detector} | {d.instance or '-'} | "
                    f"{d.real_windows} | {d.fires_on_real} | "
                    f"{d.real_fire_rate:.2%} |"
                )
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# CLI entry point (also usable via ``python -m promanomaly.backtest``)
# ---------------------------------------------------------------------------


@click.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
    help="Path to the YAML configuration file.",
)
@click.option("--group", "group_name", required=True, help="Group to backtest.")
@click.option(
    "--inject",
    is_flag=True,
    default=False,
    help="Run with synthetic-anomaly injection (precision/recall mode).",
)
@click.option(
    "--from",
    "from_",
    default=None,
    help=(
        "History start timestamp (RFC3339, unix seconds, or a relative "
        "'-7d' / '-3h' offset from --to). When omitted the lookback "
        "defaults to defaults.window (1x real, 4x inject mode)."
    ),
)
@click.option(
    "--to",
    "to_",
    default=None,
    help=("History end timestamp (RFC3339, unix seconds, or 'now'). Defaults to now."),
)
@click.option(
    "--end",
    default=None,
    help=(
        "Deprecated alias for --to, kept for backwards compatibility "
        "with shell history. Prefer --to."
    ),
)
@click.option(
    "--max-windows",
    default=_DEFAULT_MAX_WINDOWS,
    show_default=True,
    type=int,
    help="Cap on sliding sub-windows per series — keeps runtime predictable.",
)
@click.option(
    "--output",
    type=click.Choice(["json", "markdown"]),
    default="json",
    show_default=True,
    help="Output format. 'markdown' is suitable for paste-into-PR comments.",
)
def backtest_cmd(
    config_path: str,
    group_name: str,
    inject: bool,
    from_: str | None,
    to_: str | None,
    end: str | None,
    max_windows: int,
    output: str,
) -> None:
    """Run a backtest against historical TSDB data for one group.

    Without ``--inject``, reports per-detector firing rates on real
    history — useful for spotting detectors that false-positive
    excessively on the operator's production-shaped signals.

    With ``--inject``, injects each built-in anomaly pattern on top of
    every sliding window and computes precision / recall / F1 per
    detector. The single most useful pre-deploy check for a
    detector-parameter tuning PR.
    """
    cfg = load_config(Path(config_path))
    # --end remains accepted but ``--to`` is the preferred spelling
    # going forward (parity with the ``analyze`` CLI).
    if end is not None and to_ is not None:
        raise click.UsageError("pass either --to or --end, not both")
    raw_to = to_ if to_ is not None else end
    end_ts: float | None = None
    if raw_to is not None:
        try:
            end_ts = _parse_timestamp(raw_to, now=time.time())
        except ValueError as exc:
            raise click.UsageError(str(exc)) from exc
    start_ts: float | None = None
    if from_ is not None:
        anchor = end_ts if end_ts is not None else time.time()
        try:
            start_ts = _parse_timestamp(from_, now=anchor)
        except ValueError as exc:
            raise click.UsageError(str(exc)) from exc
        if end_ts is None:
            end_ts = anchor

    async def _run() -> BacktestReport:
        source = PromQLSource(
            cfg.datasource.url,
            cfg.datasource.timeout_seconds,
            auth=cfg.datasource.auth,
        )
        try:
            await source.start()
            return await run_backtest(
                config=cfg,
                source=source,
                group_name=group_name,
                inject=inject,
                end=end_ts,
                start=start_ts,
                max_windows=max_windows,
            )
        finally:
            await source.close()

    report = asyncio.run(_run())
    if output == "markdown":
        click.echo(render_markdown(report))
    else:
        click.echo(json.dumps(report.as_dict(), indent=2, sort_keys=True))


def main() -> None:
    """``python -m promanomaly.backtest`` entry point used by the Makefile."""
    backtest_cmd(prog_name="promanomaly.backtest")


if __name__ == "__main__":  # pragma: no cover - module entry
    main()


__all__ = [
    "BacktestReport",
    "DetectorMetrics",
    "QueryReport",
    "backtest_cmd",
    "render_markdown",
    "run_backtest",
]
