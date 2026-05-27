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
from typing import Any

import click

from .config import Config, load_config, parse_duration
from .detectors import get as get_detector
from .detectors import list_meta
from .exporter import OperationalMetrics
from .logging import configure_logging
from .runner import Runner, _build_detector
from .source import PromQLSource, QuerySeries, SourceQueryError
from .state import SnapshotStore


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
        for group in cfg.groups:
            for query in group.queries:
                entry: dict[str, Any] = {
                    "group": group.name,
                    "query": query.id,
                    "promql": query.promql,
                }
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
    summary = {
        "total_series": total_series,
        "max_total_series": cfg.safety.max_total_series,
        "empty_unexpected": empty,
        "errored": errored,
    }
    click.echo(json.dumps({"summary": summary}, sort_keys=True))
    if strict and (empty > 0 or errored > 0):
        return 1
    return 0


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
    default="1h",
    show_default=True,
    help="Rolling window of recent data to pull (e.g. 1h, 30m).",
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

    async def _run() -> int:
        source = PromQLSource(datasource_url, parse_duration(timeout))
        try:
            await source.start()
            result = await source.range_query(
                promql=promql,
                end=time.time(),
                window_seconds=parse_duration(window),
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
