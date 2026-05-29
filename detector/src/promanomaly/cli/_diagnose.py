"""Orchestration for the ``promanomaly diagnose`` subcommand.

Pure analysis lives in :mod:`promanomaly.diagnose`; this module does the
I/O — issuing the instant queries against the TSDB (lookback mode) or
scraping the detector's ``/metrics`` (snapshot mode) — and assembles a
:class:`promanomaly.diagnose.DiagnoseReport`.
"""

from __future__ import annotations

import re
from typing import Any

from ..config import Config
from ..diagnose import (
    DiagnoseReport,
    diagnose_from_metrics_text,
    evaluate_empty,
    evaluate_firing_rates,
    evaluate_silent_detectors,
    evaluate_warming,
)

# Restrict the lookback to a Prometheus range-duration literal so it can
# be interpolated into a ``[...]`` selector without opening a PromQL
# injection. Matches the duration grammar the config schema accepts.
_RANGE_RE = re.compile(r"^\d+(?:ms|s|m|h|d|w)$")


def validate_window(window: str) -> str:
    """Return ``window`` if it is a safe range literal, else raise ``ValueError``."""
    if not _RANGE_RE.match(window.strip()):
        raise ValueError(
            f"invalid --window {window!r}; expected a Prometheus duration like '24h', '7d'"
        )
    return window.strip()


async def run_diagnose_tsdb(
    cfg_datasource_url: str,
    timeout_seconds: float,
    *,
    window: str,
    fire_high: float,
    never_eps: float,
    source_factory: Any,
    config: Config | None,
) -> DiagnoseReport:
    """Diagnose against the TSDB holding the detector's own metrics."""
    w = validate_window(window)
    source = source_factory(cfg_datasource_url, timeout_seconds)
    findings = []
    try:
        await source.start()
        firing_rows = await source.instant_query(
            f"avg by (id, group, detector, detector_instance) "
            f"(avg_over_time(anomaly_outside_threshold[{w}]))"
        )
        findings.extend(
            evaluate_firing_rates(firing_rows, fire_high=fire_high, never_eps=never_eps)
        )
        warming_rows = await source.instant_query(
            f"count by (group, id) (min_over_time(anomaly_warming_up[{w}]) >= 1)"
        )
        findings.extend(evaluate_warming(warming_rows))
        empty_rows = await source.instant_query(f"max_over_time(anomaly_series_count[{w}])")
        findings.extend(evaluate_empty(empty_rows))
        if config is not None:
            score_rows = await source.instant_query(
                f"count by (group, detector, detector_instance) "
                f"(count_over_time(anomaly_score[{w}]))"
            )
            emitting = {
                (labels.get("group", ""), labels.get("detector", "")) for labels, _ in score_rows
            }
            findings.extend(evaluate_silent_detectors(config, emitting))
    finally:
        await source.close()
    return DiagnoseReport(mode="tsdb", window=w, findings=findings)


def run_diagnose_target(
    target_url: str,
    timeout_seconds: float,
    *,
    fire_high: float,
) -> DiagnoseReport:
    """Diagnose from one ``/metrics`` scrape of a running detector."""
    import httpx

    url = target_url.rstrip("/") + "/metrics"
    resp = httpx.get(url, timeout=timeout_seconds)
    resp.raise_for_status()
    return diagnose_from_metrics_text(resp.text, fire_high=fire_high)


__all__ = ["run_diagnose_target", "run_diagnose_tsdb", "validate_window"]
