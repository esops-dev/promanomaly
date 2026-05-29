"""promanomaly CLI.

Subcommands
-----------
- ``validate`` — parse YAML and run pydantic validation; ``--probe``
  additionally executes every query against the live datasource.
- ``dry-run`` — validate + execute every group once in-process, then exit.
- ``detect-once`` — run one detector against an ad-hoc PromQL query.
- ``analyze`` — offline change-point analysis over a historical PromQL
  range (uses ``ruptures`` Pelt/Binseg/Window algorithms).
- ``calibrate-buckets`` — autocorrelation analysis to recommend a
  stratified detector + bucket width for a given signal.
- ``inspect`` — query a running detector's /debug/inspect for one
  (id, label) series.
- ``top`` — query a running detector's /debug/anomalies for the
  currently-firing series, ranked by severity.
- ``detectors list`` — dump the entry-point registry.

Running without a subcommand boots the server (used by the container's
ENTRYPOINT). Flag names parallel promforecast where the semantics match.

The Click commands live in this module so monkeypatch hooks on
``promanomaly.cli`` (e.g. ``PromQLSource``) keep working. Pure helper
logic lives in the submodules under :mod:`promanomaly.cli` and is
re-exported here for test compatibility.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from typing import Any

import click

from ..backtest import backtest_cmd
from ..config import load_config, parse_duration
from ..detectors import get as get_detector
from ..detectors import list_meta
from ..exporter import OperationalMetrics
from ..logging import configure_logging
from ..plan import build_detector as _build_detector
from ..runner import Runner
from ..source import PromQLSource, QuerySeries, SourceQueryError
from ..state import SnapshotStore
from ..stratified import query_window_seconds_for
from ._analyze import (
    _format_iso,
    _parse_analyze_timestamp,
    _print_analyze_text,
    _run_offline_change_points,
)
from ._calibrate import (
    _analyse_bucket_structure,
    _close_to,
    _humanise_seconds,
    _print_calibrate_text,
    _recommend_detector,
    _round_to_dividing_bucket,
)
from ._common import _coerce_param, _safe_float
from ._inspect import _print_inspect_text
from ._probe import (
    DiscoverProbeResult as _DiscoverProbeResult,
)
from ._probe import (
    probe_discover_query as _probe_discover_query,
)
from ._probe import (
    run_probe as _run_probe,
)
from ._probe import (
    stratified_summary_for_query as _stratified_summary_for_query,
)
from ._top import _print_top_text


@click.group(invoke_without_command=True, context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--config",
    "config_path",
    type=click.Path(dir_okay=False),
    default=None,
    help="Path to the YAML configuration file.",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    default="INFO",
    show_default=True,
)
@click.pass_context
def cli(ctx: click.Context, config_path: str | None, log_level: str) -> None:
    """promanomaly — anomaly detection for Prometheus-compatible metrics."""
    configure_logging(log_level)
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path
    if ctx.invoked_subcommand is None:
        if not config_path:
            raise click.UsageError("--config is required when running the server")
        from ..main import serve  # local import: avoids loading uvicorn for CLI utilities

        serve(config_path)


@cli.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
)
@click.option(
    "--probe",
    is_flag=True,
    default=False,
    help=(
        "After schema validation, execute every query against the live "
        "datasource and report per-query series counts."
    ),
)
@click.option(
    "--strict",
    is_flag=True,
    default=False,
    help=(
        "With --probe, exit non-zero when any query returns zero series "
        "or errors. Suitable as a pre-deploy CI gate."
    ),
)
@click.option(
    "--datasource-url",
    default=None,
    help=(
        "Override the datasource URL for probing — useful for testing a "
        "config against a staging TSDB without editing the file."
    ),
)
def validate(
    config_path: str,
    probe: bool,
    strict: bool,
    datasource_url: str | None,
) -> None:
    """Validate a config file (and optionally probe the datasource)."""
    try:
        cfg = load_config(config_path)
    except Exception as exc:
        click.echo(f"INVALID: {exc}", err=True)
        sys.exit(1)

    if not probe:
        if strict:
            # --strict without --probe is a user error: schema validation
            # is binary (pass / fail) and already exits non-zero on fail.
            raise click.UsageError("--strict requires --probe")
        click.echo(f"OK: apiVersion={cfg.apiVersion} groups={[g.name for g in cfg.groups]}")
        return

    # Pass the cli-module's ``PromQLSource`` symbol as the factory so
    # tests that ``monkeypatch.setattr(cli, "PromQLSource", stub)`` win
    # regardless of where the probe implementation lives. Production
    # callers see the real source class.
    exit_code = asyncio.run(
        _run_probe(
            cfg,
            strict=strict,
            datasource_override=datasource_url,
            source_factory=PromQLSource,
        )
    )
    sys.exit(exit_code)


# The 450+ LOC of probe logic that used to live here moved to
# :mod:`promanomaly.cli._probe` — kept re-exported above so existing
# monkeypatch hooks (``promanomaly.cli._run_probe``, etc.) keep working.


@cli.command(name="dry-run")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
)
def dry_run(config_path: str) -> None:
    """Run every group once in-process and print a summary, then exit."""
    cfg = load_config(config_path)

    async def _run() -> int:
        source = PromQLSource(
            cfg.datasource.url,
            cfg.datasource.timeout_seconds,
            auth=cfg.datasource.auth,
        )
        store = SnapshotStore()
        ops = OperationalMetrics()
        runner = Runner(cfg, source, store, ops)
        try:
            await source.start()
            for group in cfg.groups:
                result = await runner.run_group(group.name)
                click.echo(
                    json.dumps(
                        {
                            "group": result.group,
                            "succeeded": result.succeeded,
                            "series_count": result.series_count,
                            "samples": len(result.samples),
                            "duration_seconds": round(result.duration_seconds, 3),
                        }
                    )
                )
        finally:
            await source.close()
        return 0

    sys.exit(asyncio.run(_run()))


@cli.command(name="detect-once")
@click.option("--query", "promql", required=True, help="PromQL query to execute.")
@click.option(
    "--datasource-url",
    default="http://localhost:8428/",
    show_default=True,
    help="PromQL-compatible datasource URL.",
)
@click.option(
    "--window",
    default="auto",
    show_default=True,
    help=(
        "Rolling window of recent data to pull (e.g. 1h, 30m). 'auto' "
        "uses the detector's preferred lookback when it exposes one "
        "(stratified detectors), else falls back to '1h'. Pass an "
        "explicit duration to override."
    ),
)
@click.option(
    "--step",
    default="15s",
    show_default=True,
    help="Range-query step.",
)
@click.option(
    "--detector",
    "detector_name",
    default="MAD",
    show_default=True,
    help="Detector name to run.",
)
@click.option(
    "--param",
    "params",
    multiple=True,
    help="Detector parameter as key=value (repeatable).",
)
@click.option(
    "--timeout",
    default="10s",
    show_default=True,
    help="Datasource HTTP timeout.",
)
def detect_once(
    promql: str,
    datasource_url: str,
    window: str,
    step: str,
    detector_name: str,
    params: tuple[str, ...],
    timeout: str,
) -> None:
    """Run one detector against an ad-hoc PromQL query and print scores."""
    cls = get_detector(detector_name)
    parsed_params = dict(getattr(cls, "defaults", {}) or {})
    for raw in params:
        if "=" not in raw:
            raise click.UsageError(f"--param {raw!r}: expected key=value")
        key, value = raw.split("=", 1)
        parsed_params[key.strip()] = _coerce_param(value.strip())

    # ``--window auto`` honours the detector's preferred lookback when
    # one is declared (stratified detectors). Without this, an operator
    # running ``detect-once --detector DayOfWeekMAD`` would get a 1h
    # fetch and the bucket-median baseline would collapse — same
    # operator footgun the runner's _fetch_and_plan widening exists to
    # avoid. An explicit ``--window`` always wins so power users can
    # still override.
    if window.strip().lower() == "auto":
        detector_window = query_window_seconds_for(cls, parsed_params)
        window_seconds = detector_window if detector_window is not None else 3600.0
    else:
        window_seconds = parse_duration(window)

    async def _run() -> int:
        source = PromQLSource(datasource_url, parse_duration(timeout))
        try:
            await source.start()
            result = await source.range_query(
                promql=promql,
                end=time.time(),
                window_seconds=window_seconds,
                step_seconds=parse_duration(step),
            )
        finally:
            await source.close()

        if not result.series:
            click.echo("no series returned", err=True)
            return 1

        # Use the same kwarg-forwarding builder the runner uses so
        # ``detect-once --param k=2.5`` works for detectors that consume
        # tuning knobs in ``__init__``.
        detector = _build_detector(cls, parsed_params)
        rows: list[dict[str, Any]] = []
        for series in result.series:
            try:
                row = detector.fit_score(series.samples, "", parsed_params)
            except Exception as exc:
                click.echo(
                    json.dumps({"labels": series.labels, "error": str(exc)}),
                    err=True,
                )
                continue
            latest = row.iloc[-1].to_dict()
            entry: dict[str, Any] = {"labels": series.labels}
            for key, value in latest.items():
                entry[str(key)] = _safe_float(value)
            rows.append(entry)
        click.echo(json.dumps(rows, indent=2, sort_keys=True))
        return 0

    sys.exit(asyncio.run(_run()))


@cli.command(name="analyze")
@click.option("--query", "promql", required=True, help="PromQL query to fetch and analyse.")
@click.option(
    "--from",
    "from_",
    required=True,
    help=(
        "Range start. RFC3339 (e.g. 2024-01-15T00:00:00Z), unix seconds, "
        "or a relative '-7d' / '-3h' offset from --to."
    ),
)
@click.option(
    "--to",
    "to_",
    default="now",
    show_default=True,
    help="Range end. RFC3339, unix seconds, or 'now' (default).",
)
@click.option(
    "--datasource-url",
    default="http://localhost:8428/",
    show_default=True,
    help="PromQL-compatible datasource URL.",
)
@click.option(
    "--step",
    default="30s",
    show_default=True,
    help="Sample step for the range query.",
)
@click.option(
    "--detector",
    "detector_name",
    type=click.Choice(["ruptures", "pelt", "binseg", "window"]),
    default="ruptures",
    show_default=True,
    help=(
        "Offline algorithm: 'ruptures' / 'pelt' is the default Pelt search; "
        "'binseg' uses binary segmentation; 'window' uses a sliding-window "
        "scan. All three are from the ruptures library."
    ),
)
@click.option(
    "--model",
    default="l2",
    show_default=True,
    help="Cost model passed to ruptures (l1, l2, rbf, ar, normal).",
)
@click.option(
    "--penalty",
    default=10.0,
    show_default=True,
    type=float,
    help="Penalty parameter (used by Pelt/Binseg); higher = fewer change-points.",
)
@click.option(
    "--top",
    default=10,
    show_default=True,
    type=int,
    help="Print at most this many change-points (ranked by score).",
)
@click.option(
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format. 'json' is suitable for machine consumption.",
)
@click.option(
    "--timeout",
    default="30s",
    show_default=True,
    help="Datasource HTTP timeout.",
)
def analyze(
    promql: str,
    from_: str,
    to_: str,
    datasource_url: str,
    step: str,
    detector_name: str,
    model: str,
    penalty: float,
    top: int,
    output: str,
    timeout: str,
) -> None:
    """Offline change-point analysis over a historical PromQL range.

    Pulls the full ``[from, to]`` range in one query, runs the chosen
    offline algorithm, and prints a ranked list of detected breaks with
    timestamps and per-break scores. Different algorithm class from
    BOCPD / CUSUM — those run online per scrape; this one knows the
    whole series at once and finds segmentation that minimises a cost
    function (Pelt / Binseg) or peak detection (Window).
    """
    try:
        to_seconds = _parse_analyze_timestamp(to_, now=time.time())
        from_seconds = _parse_analyze_timestamp(from_, now=to_seconds)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc
    if from_seconds >= to_seconds:
        raise click.UsageError("--from must be before --to")

    step_seconds = parse_duration(step)
    window_seconds = to_seconds - from_seconds

    async def _fetch() -> list[QuerySeries]:
        source = PromQLSource(datasource_url, parse_duration(timeout))
        try:
            await source.start()
            result = await source.range_query(
                promql=promql,
                end=to_seconds,
                window_seconds=window_seconds,
                step_seconds=step_seconds,
            )
        finally:
            await source.close()
        return list(result.series)

    try:
        series_list = asyncio.run(_fetch())
    except SourceQueryError as exc:
        click.echo(f"query failed: {exc}", err=True)
        sys.exit(2)

    if not series_list:
        click.echo("no series returned", err=True)
        sys.exit(1)

    findings = _run_offline_change_points(
        series_list=series_list,
        detector_name=detector_name,
        model=model,
        penalty=penalty,
        top=top,
    )

    if output == "json":
        click.echo(json.dumps(findings, indent=2, sort_keys=True))
    else:
        _print_analyze_text(findings)


@cli.command(name="calibrate-buckets")
@click.option("--query", "promql", required=True, help="PromQL query to analyse.")
@click.option(
    "--from",
    "from_",
    required=True,
    help=(
        "Range start. RFC3339 (e.g. 2024-01-15T00:00:00Z), unix seconds, "
        "or a relative '-14d' / '-3h' offset from --to."
    ),
)
@click.option(
    "--to",
    "to_",
    default="now",
    show_default=True,
    help="Range end. RFC3339, unix seconds, or 'now' (default).",
)
@click.option(
    "--datasource-url",
    default="http://localhost:8428/",
    show_default=True,
    help="PromQL-compatible datasource URL.",
)
@click.option(
    "--step",
    default="5m",
    show_default=True,
    help=(
        "Sample step for the range query. The autocorrelation analysis "
        "is performed at this resolution; a 5m step over a 14d window "
        "keeps the calculation cheap while still resolving sub-hour "
        "structure."
    ),
)
@click.option(
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help=(
        "Output format. 'text' prints the recommended detector + a "
        "ready-to-paste YAML snippet; 'json' is the raw analysis."
    ),
)
@click.option(
    "--timeout",
    default="30s",
    show_default=True,
    help="Datasource HTTP timeout.",
)
def calibrate_buckets(
    promql: str,
    from_: str,
    to_: str,
    datasource_url: str,
    step: str,
    output: str,
    timeout: str,
) -> None:
    """Suggest the right stratified-baseline bucketing for a signal.

    Pulls the historical range, runs an autocorrelation analysis at the
    requested step, and reports the dominant periodicity. Maps the
    finding onto one of the stratified detector configurations
    (HourOfDayMAD when daily structure dominates, DayOfWeekMAD when a
    weekly pattern is layered on top) and prints a ready-to-paste YAML
    snippet so the operator can drop it straight into their config.
    """
    try:
        to_seconds = _parse_analyze_timestamp(to_, now=time.time())
        from_seconds = _parse_analyze_timestamp(from_, now=to_seconds)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc
    if from_seconds >= to_seconds:
        raise click.UsageError("--from must be before --to")

    step_seconds = parse_duration(step)
    window_seconds = to_seconds - from_seconds

    async def _fetch() -> list[QuerySeries]:
        source = PromQLSource(datasource_url, parse_duration(timeout))
        try:
            await source.start()
            result = await source.range_query(
                promql=promql,
                end=to_seconds,
                window_seconds=window_seconds,
                step_seconds=step_seconds,
            )
        finally:
            await source.close()
        return list(result.series)

    try:
        series_list = asyncio.run(_fetch())
    except SourceQueryError as exc:
        click.echo(f"query failed: {exc}", err=True)
        sys.exit(2)

    if not series_list:
        click.echo("no series returned", err=True)
        sys.exit(1)

    analysis = _analyse_bucket_structure(
        series_list=series_list,
        step_seconds=step_seconds,
        window_seconds=window_seconds,
    )

    if output == "json":
        click.echo(json.dumps(analysis, indent=2, sort_keys=True))
        return
    _print_calibrate_text(analysis)


@cli.command(name="inspect")
@click.option(
    "--target",
    "target_url",
    required=True,
    help="Running detector base URL (e.g. http://localhost:9092).",
)
@click.option(
    "--id",
    "metric_id",
    required=True,
    help="Metric id to inspect (matches a configured query.id).",
)
@click.option(
    "--labels",
    "labels",
    default=None,
    help="Label selector as 'k1=v1,k2=v2'. Empty matches every series for the id.",
)
@click.option(
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format. 'json' is the raw /debug/inspect response.",
)
@click.option(
    "--timeout",
    default=10.0,
    show_default=True,
    help="Datasource HTTP timeout.",
)
def inspect_cmd(
    target_url: str,
    metric_id: str,
    labels: str | None,
    output: str,
    timeout: float,
) -> None:
    """Query a running detector's /debug/inspect for one (id, label) series.

    The highest-leverage answer to "why did this series score 4.7?" —
    pulls the live rolling window the detector would see right now, runs
    every configured detector against it, and prints the per-detector
    score / baseline / threshold along with calibration data and
    warm-up status.
    """
    import httpx

    params: dict[str, str] = {"id": metric_id}
    if labels:
        params["labels"] = labels
    url = target_url.rstrip("/") + "/debug/inspect"
    try:
        resp = httpx.get(url, params=params, timeout=timeout)
    except httpx.HTTPError as exc:
        click.echo(f"could not reach {target_url}: {exc}", err=True)
        sys.exit(2)
    try:
        payload = resp.json()
    except ValueError:
        click.echo(f"non-JSON response from {url}: {resp.text[:200]}", err=True)
        sys.exit(2)
    if resp.status_code >= 400:
        click.echo(json.dumps(payload, indent=2, sort_keys=True), err=True)
        sys.exit(1)
    if output == "json":
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    _print_inspect_text(payload)


@cli.command(name="top")
@click.option(
    "--target",
    "target_url",
    required=True,
    help="Running detector base URL (e.g. http://localhost:9092).",
)
@click.option(
    "--group",
    "group",
    default=None,
    help="Restrict to one group.",
)
@click.option(
    "--min-severity",
    "min_severity",
    default=0.0,
    show_default=True,
    type=float,
    help="Drop firings below this severity (0-1).",
)
@click.option(
    "--limit",
    "limit",
    default=20,
    show_default=True,
    type=int,
    help="Show at most this many series (ranked by severity).",
)
@click.option(
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format. 'json' is the raw /debug/anomalies response.",
)
@click.option(
    "--timeout",
    default=10.0,
    show_default=True,
    help="HTTP timeout.",
)
def top_cmd(
    target_url: str,
    group: str | None,
    min_severity: float,
    limit: int,
    output: str,
    timeout: float,
) -> None:
    """List what is anomalous right now, ranked by severity.

    Wraps a running detector's /debug/anomalies endpoint — like ``top``
    on a host, but for the fleet. Pairs with ``inspect`` as a two-step
    triage flow: ``top`` to find the worst series, ``inspect`` to
    understand *why* it scored.
    """
    import httpx

    params: dict[str, str] = {"min_severity": str(min_severity), "limit": str(limit)}
    if group:
        params["group"] = group
    url = target_url.rstrip("/") + "/debug/anomalies"
    try:
        resp = httpx.get(url, params=params, timeout=timeout)
    except httpx.HTTPError as exc:
        click.echo(f"could not reach {target_url}: {exc}", err=True)
        sys.exit(2)
    try:
        payload = resp.json()
    except ValueError:
        click.echo(f"non-JSON response from {url}: {resp.text[:200]}", err=True)
        sys.exit(2)
    if resp.status_code >= 400:
        click.echo(json.dumps(payload, indent=2, sort_keys=True), err=True)
        sys.exit(1)
    if output == "json":
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    _print_top_text(payload)


# Register the backtest command imported from promanomaly.backtest so
# operators can invoke it as ``promanomaly backtest`` rather than the
# longer ``python -m promanomaly.backtest`` (which still works because
# the module retains its own entry point for Makefile use).
cli.add_command(backtest_cmd, name="backtest")


@cli.group()
def detectors() -> None:
    """Introspect installed detectors."""


@detectors.command(name="list")
@click.option(
    "--target",
    "target_url",
    default=None,
    help="Query a running detector's /debug/detectors endpoint over HTTP.",
)
def detectors_list(target_url: str | None) -> None:
    """List installed detectors with their parameters and defaults."""
    if target_url is not None:
        import httpx

        try:
            resp = httpx.get(target_url.rstrip("/") + "/debug/detectors", timeout=5)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            click.echo(f"could not reach {target_url}: {exc}", err=True)
            sys.exit(2)
        click.echo(resp.text)
        return

    rows = [meta.as_dict() for meta in list_meta()]
    click.echo(json.dumps(rows, indent=2, sort_keys=True))


def main() -> None:
    """Console-script entrypoint."""
    cli(prog_name="promanomaly", standalone_mode=True)


# Re-exports for backward compatibility with tests that monkeypatch
# ``promanomaly.cli.<helper>`` or import helpers directly.
__all__ = [
    "_DiscoverProbeResult",
    "_analyse_bucket_structure",
    "_close_to",
    "_coerce_param",
    "_format_iso",
    "_humanise_seconds",
    "_parse_analyze_timestamp",
    "_print_analyze_text",
    "_print_calibrate_text",
    "_print_inspect_text",
    "_print_top_text",
    "_probe_discover_query",
    "_recommend_detector",
    "_round_to_dividing_bucket",
    "_run_offline_change_points",
    "_run_probe",
    "_safe_float",
    "_stratified_summary_for_query",
    "cli",
    "main",
]


if __name__ == "__main__":
    main()
