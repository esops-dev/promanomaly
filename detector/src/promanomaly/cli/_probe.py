"""``validate --probe`` helpers extracted from :mod:`promanomaly.cli`.

The Click command itself stays in :mod:`promanomaly.cli` so that
``promanomaly.cli.<helper>`` monkeypatch hooks used by the test suite
keep working; this module holds the pure logic so the CLI module
shrinks back to the command-definition shape it advertises in its
docstring.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

import click

from ..config import Config
from ..detectors import get as get_detector
from ..discovery import DiscoveryError, expand_query, probe_variable
from ..source import PromQLSource, SourceQueryError
from ..stratified import stratified_cost_summary


def stratified_summary_for_query(query: Any, cfg: Config) -> dict[str, Any] | None:
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
class DiscoverProbeResult:
    """Accumulator returned by :func:`probe_discover_query`."""

    variant_count: int = 0
    total_series: int = 0
    empty_unexpected: int = 0
    errored: int = 0


async def probe_discover_query(
    *,
    source: PromQLSource,
    group_name: str,
    query: Any,
    probe_fn: Any,
    end: float,
    window: float,
    step: float,
    max_series: int,
    stratified: dict[str, Any] | None = None,
) -> DiscoverProbeResult:
    """Resolve discovery probes, expand, execute, and emit JSON lines.

    When ``stratified`` is supplied (the template has at least one
    detector opted into the sliding-window strategy), each rendered
    variant carries the same stratified-cost projection — operators see
    the per-rendered-id TSDB cost as well as the template-level total.
    """
    result = DiscoverProbeResult()
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


async def run_probe(
    cfg: Config,
    *,
    strict: bool,
    datasource_override: str | None,
    source_factory: Any = None,
) -> int:
    """Execute every configured query against the live datasource.

    Reports one JSON line per query with the series count or an error
    reason. Under ``--strict``, exits non-zero when any query is empty
    or errors — the intended pre-deploy CI gate. Without ``--strict``,
    only schema-validation failures or transport-level failures abort.

    ``source_factory`` lets the cli layer inject a constructor that
    honours module-level monkeypatches on ``promanomaly.cli.PromQLSource``
    (the historical hook used by the test suite). Defaults to the real
    :class:`PromQLSource` for production use.
    """
    url = datasource_override or cfg.datasource.url
    factory = source_factory or PromQLSource
    source = factory(url, cfg.datasource.timeout_seconds, auth=cfg.datasource.auth)
    empty = 0
    errored = 0
    total_series = 0
    discovered_total = 0
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

        for group in cfg.groups:
            for query in group.queries:
                if query.discover:
                    stratified_for_template = stratified_summary_for_query(query, cfg)
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
                    discovered_count = await probe_discover_query(
                        source=source,
                        group_name=group.name,
                        query=query,
                        probe_fn=_probe,
                        end=end,
                        window=window,
                        step=step,
                        max_series=cfg.safety.max_series_per_query,
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
                stratified = stratified_summary_for_query(query, cfg)
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


__all__ = [
    "DiscoverProbeResult",
    "probe_discover_query",
    "run_probe",
    "stratified_summary_for_query",
]
