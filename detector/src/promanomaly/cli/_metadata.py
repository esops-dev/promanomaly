"""``validate --lint-metadata`` — metric-type mismatch lint.

Queries the datasource's ``/api/v1/metadata`` and warns when a query
feeds a raw counter to a detector without a ``rate()`` / ``increase()``
wrapper. Lint only: ``--strict`` turns findings into a non-zero exit so
it can gate CI, but it never rewrites the query. Composes with
``--probe`` and ``--estimate-cost``.
"""

from __future__ import annotations

import json
from typing import Any

import click

from ..config import Config
from ..metadata_lint import lint_config_metadata
from ..source import SourceQueryError


async def run_metadata_lint(
    cfg: Config,
    *,
    strict: bool,
    datasource_override: str | None,
    source_factory: Any,
) -> int:
    """Fetch metadata, lint the config, print findings; return an exit code.

    One ``{"status": "lint", ...}`` line per finding, then a
    ``{"summary": {...}}`` line. ``strict`` makes any finding exit
    non-zero. A datasource that can't be reached is reported and, under
    ``strict``, exits non-zero (so a broken pre-deploy check fails loud
    rather than silently passing).
    """
    url = datasource_override or cfg.datasource.url
    source = source_factory(
        url,
        cfg.datasource.timeout_seconds,
        auth=cfg.datasource.auth,
    )
    try:
        await source.start()
        try:
            metadata = await source.metric_metadata()
        except SourceQueryError as exc:
            click.echo(
                json.dumps(
                    {"status": "metadata_unavailable", "reason": exc.reason, "detail": exc.message},
                    sort_keys=True,
                ),
                err=True,
            )
            # Lint is advisory; an unreachable datasource is only fatal
            # under --strict, where the operator asked for a hard gate.
            return 1 if strict else 0
    finally:
        await source.close()

    findings = lint_config_metadata(cfg, metadata)
    for finding in findings:
        click.echo(json.dumps({"status": "lint", **finding.to_dict()}, sort_keys=True))
    click.echo(
        json.dumps(
            {
                "summary": {
                    "metrics_with_metadata": len(metadata),
                    "findings": len(findings),
                }
            },
            sort_keys=True,
        )
    )
    if findings and strict:
        return 1
    return 0


__all__ = ["run_metadata_lint"]
