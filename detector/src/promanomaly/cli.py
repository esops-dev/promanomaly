"""promanomaly CLI.

Subcommands
-----------
- ``validate`` — parse YAML and run pydantic validation; ``--probe``
  additionally executes every query against the live datasource.
- ``dry-run`` — validate + execute every group once in-process, then exit.
- ``detect-once`` — run one detector against an ad-hoc PromQL query.
- ``analyze`` — offline change-point analysis over a historical PromQL
  range (uses ``ruptures`` Pelt/Binseg/Window algorithms).
- ``detectors list`` — dump the entry-point registry.

Running without a subcommand boots the server (used by the container's
ENTRYPOINT). Flag names parallel promforecast where the semantics match.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import json
import sys
import time
from dataclasses import dataclass
from typing import Any

import click

from .backtest import backtest_cmd
from .config import Config, load_config, parse_duration
from .detectors import get as get_detector
from .detectors import list_meta
from .discovery import DiscoveryError, expand_query, probe_variable
from .exporter import OperationalMetrics
from .logging import configure_logging
from .runner import Runner, _build_detector
from .source import PromQLSource, QuerySeries, SourceQueryError
from .state import SnapshotStore
from .stratified import query_window_seconds_for, stratified_cost_summary


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
        from .main import serve  # local import: avoids loading uvicorn for CLI utilities

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

    exit_code = asyncio.run(_run_probe(cfg, strict=strict, datasource_override=datasource_url))
    sys.exit(exit_code)


async def _run_probe(
    cfg: Config,
    *,
    strict: bool,
    datasource_override: str | None,
) -> int:
    """Execute every configured query against the live datasource.

    Reports one JSON line per query with the series count or an error
    reason. Under ``--strict``, exits non-zero when any query is empty
    or errors — the intended pre-deploy CI gate. Without ``--strict``,
    only schema-validation failures or transport-level failures abort.
    """
    url = datasource_override or cfg.datasource.url
    source = PromQLSource(url, cfg.datasource.timeout_seconds, auth=cfg.datasource.auth)
    empty = 0
    errored = 0
    total_series = 0
    try:
        try:
            await source.start()
        except Exception as exc:
            # Connection / cert / config-level failure before any query
            # has even run. Surface a clean error line + summary rather
            # than dumping a traceback at the operator, and exit 1
            # regardless of --strict (failure here means we couldn't
            # probe anything at all).
            click.echo(
                json.dumps(
                    {"status": "datasource_unreachable", "url": url, "detail": str(exc)},
                    sort_keys=True,
                )
            )
            click.echo(
                json.dumps(
                    {
                        "summary": {
                            "total_series": 0,
                            "max_total_series": cfg.safety.max_total_series,
                            "empty_unexpected": 0,
                            "errored": 0,
                            "datasource_unreachable": True,
                        }
                    },
                    sort_keys=True,
                )
            )
            return 1
        end = time.time()
        window = cfg.defaults.window_seconds
        step = cfg.defaults.step_seconds

        async def _probe(promql: str) -> Any:
            return await source.range_query(
                promql=promql,
                end=end,
                window_seconds=window,
                step_seconds=step,
                max_series=cfg.safety.max_series_per_query,
            )

        discovered_total = 0
        for group in cfg.groups:
            for query in group.queries:
                if query.discover:
                    # Discovery-expansion simulation: probe each
                    # variable, run the Cartesian expansion, then
                    # execute each rendered variant. Reports the
                    # projected total series so operators see at
                    # PR-time what would land in production.
                    # Stratified detectors layered on top of discover:
                    # blocks need the same TSDB-cost projection that
                    # non-discover queries get — without this, the
                    # most expensive combination (stratified x
                    # discovery-fan-out) is the one with no PR-time
                    # cost signal.
                    stratified_for_template = _stratified_summary_for_query(query, cfg)
                    if stratified_for_template is not None:
                        click.echo(
                            json.dumps(
                                {
                                    "group": group.name,
                                    "query": query.id,
                                    "status": "stratified_template",
                                    "stratified": stratified_for_template,
                                },
                                sort_keys=True,
                            )
                        )
                    discovered_count = await _probe_discover_query(
                        source=source,
                        group_name=group.name,
                        query=query,
                        probe_fn=_probe,
                        end=end,
                        window=window,
                        step=step,
                        max_series=cfg.safety.max_series_per_query,
                        counters=lambda kind: None,
                        stratified=stratified_for_template,
                    )
                    if discovered_count.errored:
                        errored += discovered_count.errored
                    if discovered_count.empty_unexpected:
                        empty += discovered_count.empty_unexpected
                    total_series += discovered_count.total_series
                    discovered_total += discovered_count.variant_count
                    continue
                entry: dict[str, Any] = {
                    "group": group.name,
                    "query": query.id,
                    "promql": query.promql,
                }
                stratified = _stratified_summary_for_query(query, cfg)
                if stratified is not None:
                    entry["stratified"] = stratified
                try:
                    result = await source.range_query(
                        promql=query.promql,
                        end=end,
                        window_seconds=window,
                        step_seconds=step,
                        max_series=cfg.safety.max_series_per_query,
                    )
                except SourceQueryError as exc:
                    entry["status"] = "error"
                    entry["reason"] = exc.reason
                    entry["detail"] = exc.message
                    errored += 1
                else:
                    series_count = len(result.series)
                    total_series += series_count
                    entry["status"] = "ok"
                    entry["series"] = series_count
                    entry["truncated"] = result.truncated
                    if series_count == 0:
                        # Empty results aren't always wrong (e.g. expect:true
                        # queries) but they are the most common deployment
                        # foot-gun — surface them at probe time.
                        entry["empty"] = True
                        if not query.expect:
                            empty += 1
                click.echo(json.dumps(entry, sort_keys=True))
    finally:
        await source.close()
    summary: dict[str, Any] = {
        "total_series": total_series,
        "max_total_series": cfg.safety.max_total_series,
        "empty_unexpected": empty,
        "errored": errored,
    }
    if discovered_total:
        summary["discovery_expansions"] = discovered_total
    click.echo(json.dumps({"summary": summary}, sort_keys=True))
    if strict and (empty > 0 or errored > 0):
        return 1
    # Discovery expansion vs the global cardinality cap — the most
    # common operator footgun documented in the v0.4 story.
    if strict and total_series > cfg.safety.max_total_series:
        click.echo(
            json.dumps(
                {
                    "status": "cardinality_exceeded",
                    "total_series": total_series,
                    "max_total_series": cfg.safety.max_total_series,
                },
                sort_keys=True,
            ),
            err=True,
        )
        return 1
    return 0


def _stratified_summary_for_query(query: Any, cfg: Config) -> dict[str, Any] | None:
    """Thin wrapper around :func:`stratified_cost_summary` for the CLI.

    Bridges the CLI's ``cfg`` object to the shared helper's
    ``refresh_interval_seconds`` argument so the cli and runner never
    drift on what counts as a stratified detector.
    """
    return stratified_cost_summary(
        query=query,
        resolve_cls=get_detector,
        refresh_interval_seconds=cfg.server.refresh_interval_seconds,
    )


@dataclass
class _DiscoverProbeResult:
    """Accumulator returned by ``_probe_discover_query``."""

    variant_count: int = 0
    total_series: int = 0
    empty_unexpected: int = 0
    errored: int = 0


async def _probe_discover_query(
    *,
    source: PromQLSource,
    group_name: str,
    query: Any,
    probe_fn: Any,
    end: float,
    window: float,
    step: float,
    max_series: int,
    counters: Any,
    stratified: dict[str, Any] | None = None,
) -> _DiscoverProbeResult:
    """Resolve discovery probes, expand, execute, and emit JSON lines.

    When ``stratified`` is supplied (the template has at least one
    detector opted into the sliding-window strategy), each rendered
    variant carries the same stratified-cost projection — operators see
    the per-rendered-id TSDB cost as well as the template-level total.
    """
    del counters
    result = _DiscoverProbeResult()
    value_sets: dict[str, list[str]] = {}
    for var in query.discover:
        try:
            values, dropped = await probe_variable(var, probe_fn=probe_fn)
        except DiscoveryError as exc:
            click.echo(
                json.dumps(
                    {
                        "group": group_name,
                        "query": query.id,
                        "variable": var.variable,
                        "status": "discovery_error",
                        "reason": exc.reason,
                        "detail": exc.message,
                    },
                    sort_keys=True,
                )
            )
            result.errored += 1
            return result
        value_sets[var.variable] = values
        entry: dict[str, Any] = {
            "group": group_name,
            "query": query.id,
            "variable": var.variable,
            "status": "discovered",
            "values": values,
            "count": len(values),
        }
        if dropped:
            # Surface unsafe values so the operator can spot a label
            # that contains characters PromQL won't accept un-escaped.
            entry["dropped_unsafe"] = dropped
        click.echo(json.dumps(entry, sort_keys=True))
    try:
        variants = expand_query(query, value_sets)
    except DiscoveryError as exc:
        click.echo(
            json.dumps(
                {
                    "group": group_name,
                    "query": query.id,
                    "status": "expansion_error",
                    "reason": exc.reason,
                    "detail": exc.message,
                },
                sort_keys=True,
            )
        )
        result.errored += 1
        return result
    result.variant_count = len(variants)
    click.echo(
        json.dumps(
            {
                "group": group_name,
                "query": query.id,
                "status": "expansion",
                "variants": len(variants),
            },
            sort_keys=True,
        )
    )
    for variant in variants:
        entry = {
            "group": group_name,
            "query": variant.rendered_id,
            "promql": variant.rendered_promql,
            "discovered_labels": variant.labels,
        }
        if stratified is not None:
            entry["stratified"] = stratified
        try:
            qresult = await source.range_query(
                promql=variant.rendered_promql,
                end=end,
                window_seconds=window,
                step_seconds=step,
                max_series=max_series,
            )
        except SourceQueryError as exc:
            entry["status"] = "error"
            entry["reason"] = exc.reason
            entry["detail"] = exc.message
            result.errored += 1
        else:
            series_count = len(qresult.series)
            result.total_series += series_count
            entry["status"] = "ok"
            entry["series"] = series_count
            entry["truncated"] = qresult.truncated
            if series_count == 0:
                entry["empty"] = True
                if not query.expect:
                    result.empty_unexpected += 1
        click.echo(json.dumps(entry, sort_keys=True))
    return result


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


def _parse_analyze_timestamp(value: str, *, now: float) -> float:
    """Parse RFC3339 / unix-seconds / 'now' / relative '-7d' to unix seconds."""
    value = value.strip()
    if value == "now":
        return float(now)
    if value.startswith("-"):
        # Relative offset from ``now`` (which the caller passes as the
        # already-parsed --to value when resolving --from).
        return float(now) - parse_duration(value[1:])
    # Try unix seconds first (integer or float).
    try:
        return float(value)
    except ValueError:
        pass
    # RFC3339; accept the common 'Z' suffix that fromisoformat() handles
    # natively as of Python 3.11.
    try:
        dt = _dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"invalid timestamp {value!r}; expected RFC3339, unix seconds, 'now', or '-7d'"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.UTC)
    return dt.timestamp()


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


def _analyse_bucket_structure(
    *,
    series_list: list[QuerySeries],
    step_seconds: float,
    window_seconds: float,
) -> dict[str, Any]:
    """Autocorrelation analysis: identify dominant periods + recommend a detector.

    Aggregates across every returned series — operators typically point
    this at a single PromQL that already filters to the metric they care
    about, but if the result has multiple series we want the cohort-wide
    pattern. The detector recommendation only fires when the dominant
    period sits near a known stratified-detector lag (24h, 7d). Anything
    else returns ``None`` for the recommendation so we don't pretend a
    random ACF peak is a real periodicity.
    """
    import numpy as np

    # Stack series into a 2D array padded with NaNs, then take the
    # nan-mean per timestamp so series that don't share every sample
    # don't break the calculation.
    columns: list[np.ndarray] = []
    for series in series_list:
        values = series.samples["y"].to_numpy(dtype=float, copy=False)
        if values.size:
            columns.append(values)
    if not columns:
        return {"status": "empty", "recommended_detector": None}
    max_len = max(c.size for c in columns)
    stacked = np.full((len(columns), max_len), np.nan, dtype=float)
    for idx, col in enumerate(columns):
        stacked[idx, : col.size] = col
    aggregated = np.nanmean(stacked, axis=0)
    aggregated = aggregated[~np.isnan(aggregated)]
    if aggregated.size < 8:
        return {
            "status": "too_few_samples",
            "samples": int(aggregated.size),
            "recommended_detector": None,
        }

    centred = aggregated - float(np.mean(aggregated))
    variance = float(np.dot(centred, centred))
    # Float noise on a strictly-constant input still produces a tiny
    # positive variance; treat anything below numerical noise as
    # constant so the recommendation surface doesn't claim periodicity
    # on a flat signal.
    if variance <= 1e-9:
        return {
            "status": "constant_signal",
            "samples": int(aggregated.size),
            "recommended_detector": None,
        }
    # Cap the lag scan to half the sample count — beyond that the ACF
    # estimator gets noisy as the overlap shrinks. We also stop short
    # of one week if the window doesn't cover it.
    max_lag = min(aggregated.size // 2, int(8 * 86400.0 / step_seconds))
    acf = np.zeros(max_lag, dtype=float)
    for lag in range(1, max_lag):
        acf[lag] = float(np.dot(centred[lag:], centred[:-lag])) / variance

    # Find local maxima in the ACF — those are candidate periods. Skip
    # the very-short-lag noise (< step minutes) which is autocorrelation
    # from smoothing, not real periodicity.
    candidates: list[tuple[int, float]] = []
    min_lag = max(2, int(900.0 / step_seconds))  # at least 15 minutes
    for lag in range(min_lag, max_lag - 1):
        if acf[lag] > acf[lag - 1] and acf[lag] > acf[lag + 1] and acf[lag] > 0.1:
            candidates.append((lag, acf[lag]))
    candidates.sort(key=lambda r: r[1], reverse=True)
    top_candidates = candidates[:5]

    dominant_period_seconds: float | None = None
    dominant_strength = 0.0
    if top_candidates:
        dominant_period_seconds = top_candidates[0][0] * step_seconds
        dominant_strength = top_candidates[0][1]

    recommendation = _recommend_detector(
        dominant_period_seconds=dominant_period_seconds,
        candidates=[(lag * step_seconds, score) for lag, score in top_candidates],
        window_seconds=window_seconds,
    )

    return {
        "status": "ok",
        "samples": int(aggregated.size),
        "step_seconds": float(step_seconds),
        "window_seconds": float(window_seconds),
        "dominant_period_seconds": dominant_period_seconds,
        "dominant_strength": dominant_strength,
        "top_candidates": [
            {
                "period_seconds": lag * step_seconds,
                "period_label": _humanise_seconds(lag * step_seconds),
                "strength": float(score),
            }
            for lag, score in top_candidates
        ],
        "recommended_detector": recommendation,
    }


_DAY = 86400.0
_WEEK = 7 * _DAY
_HOUR = 3600.0


def _recommend_detector(
    *,
    dominant_period_seconds: float | None,
    candidates: list[tuple[float, float]],
    window_seconds: float,
) -> dict[str, Any] | None:
    """Map the dominant ACF period onto a stratified detector config."""
    if dominant_period_seconds is None:
        return None
    weekly_present = any(_close_to(period, _WEEK, tol=0.1) for period, _ in candidates)
    daily_present = any(_close_to(period, _DAY, tol=0.1) for period, _ in candidates)
    has_weekly_window = window_seconds >= 2 * _WEEK

    if weekly_present and has_weekly_window:
        return {
            "name": "DayOfWeekMAD",
            "reason": (
                "Weekly periodicity detected; (day-of-week, hour) "
                "stratification matches the signal shape."
            ),
            "params": {"lookback": "4w", "bucket_width": "1h"},
            "config_snippet": (
                "detectors:\n"
                "  - name: DayOfWeekMAD\n"
                "    params:\n"
                "      lookback: 4w\n"
                "      bucket_width: 1h\n"
            ),
        }
    if daily_present:
        return {
            "name": "HourOfDayMAD",
            "reason": (
                "Daily periodicity detected; hour-of-day stratification "
                "captures the dominant structure."
            ),
            "params": {"lookback": "7d", "bucket_width": "1h"},
            "config_snippet": (
                "detectors:\n"
                "  - name: HourOfDayMAD\n"
                "    params:\n"
                "      lookback: 7d\n"
                "      bucket_width: 1h\n"
            ),
        }
    # Sub-hour periodicity → fine-grained bucket width with the
    # short-lookback hour-of-day detector; user gets a starting point
    # even when the signal doesn't fit the canonical 24h/7d shapes.
    if dominant_period_seconds < _HOUR and dominant_period_seconds > 0:
        bucket = _round_to_dividing_bucket(dominant_period_seconds)
        return {
            "name": "HourOfDayMAD",
            "reason": (
                f"Sub-hour periodicity detected ({_humanise_seconds(dominant_period_seconds)}); "
                "narrow the bucket_width to match."
            ),
            "params": {"lookback": "7d", "bucket_width": bucket},
            "config_snippet": (
                "detectors:\n"
                "  - name: HourOfDayMAD\n"
                "    params:\n"
                "      lookback: 7d\n"
                f"      bucket_width: {bucket}\n"
            ),
        }
    return None


def _close_to(value: float, target: float, *, tol: float) -> bool:
    """True when ``value`` is within ``tol`` (relative) of ``target``."""
    if target == 0.0:
        return value == 0.0
    return abs(value - target) / target <= tol


def _round_to_dividing_bucket(period_seconds: float) -> str:
    """Pick a bucket_width that divides 24h and is close to ``period_seconds``."""
    # Sorted ascending so we always pick the narrower bucket on a tie.
    candidates_s = [60, 300, 600, 900, 1800, 3600]
    best = min(candidates_s, key=lambda c: abs(c - period_seconds))
    if best < 3600 and best % 60 == 0:
        minutes = best // 60
        return f"{minutes}m"
    return "1h"


def _humanise_seconds(seconds: float) -> str:
    if seconds >= _WEEK:
        return f"{seconds / _WEEK:.1f}w"
    if seconds >= _DAY:
        return f"{seconds / _DAY:.1f}d"
    if seconds >= _HOUR:
        return f"{seconds / _HOUR:.1f}h"
    if seconds >= 60.0:
        return f"{seconds / 60.0:.1f}m"
    return f"{seconds:.1f}s"


def _print_calibrate_text(analysis: dict[str, Any]) -> None:
    status = analysis.get("status")
    if status != "ok":
        click.echo(f"calibration failed: {status}")
        return
    click.echo(f"samples analysed       : {analysis['samples']}")
    click.echo(f"step                   : {_humanise_seconds(analysis['step_seconds'])}")
    click.echo(f"window                 : {_humanise_seconds(analysis['window_seconds'])}")
    dominant = analysis.get("dominant_period_seconds")
    if dominant:
        click.echo(
            f"dominant period        : {_humanise_seconds(dominant)} "
            f"(strength {analysis['dominant_strength']:.3f})"
        )
    candidates = analysis.get("top_candidates") or []
    if candidates:
        click.echo("top candidate periods  :")
        for cand in candidates:
            click.echo(f"  - {cand['period_label']:>8}   strength {cand['strength']:.3f}")
    rec = analysis.get("recommended_detector")
    if rec is None:
        click.echo("recommended detector   : (no stratified detector clearly fits)")
        click.echo(
            "  Consider MAD / Hampel on the rolling window, or rerun with a longer --from window."
        )
        return
    click.echo(f"recommended detector   : {rec['name']}")
    click.echo(f"reason                 : {rec['reason']}")
    click.echo("")
    click.echo("Paste into your group's query:")
    click.echo("")
    click.echo(rec["config_snippet"].rstrip())


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
    type=float,
    help="HTTP timeout in seconds.",
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


def main() -> None:
    """Console-script entrypoint."""
    cli(prog_name="promanomaly", standalone_mode=True)


if __name__ == "__main__":
    main()
