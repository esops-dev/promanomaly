"""``generate-rules`` — scaffold PrometheusRules from a detector config.

Emits a ``PrometheusRule`` skeleton covering every configured group and
query, with the ``id`` / ``group`` selectors already filled in from the
config so an operator never hand-copies a label set. Severities, ``for:``
windows, and any other policy knobs are left as clearly-marked ``TODO``
placeholders — the project deliberately ships no opinionated thresholds,
so this is scaffolding, not a recommendation.

Each group also gets the ``max``-ensemble recording-rule fallback
(``anomaly_outside_threshold_any``) described in ``docs/patterns.md``, so
operators who don't use the built-in ``ensemble:`` block still get the
group-level OR rollup for free.

The output is plain YAML (not a Helm template), written with inline
comments, so the generated file is immediately reviewable and appliable.
The intended workflow pairs with the GitOps validation Action: generate,
edit the ``TODO`` placeholders, validate, commit.
"""

from __future__ import annotations

import re

from ..config import Config, GroupConfig, QueryConfig

_NON_ALNUM_RE = re.compile(r"[^0-9A-Za-z]+")


def _camel(text: str) -> str:
    """PascalCase a snake/space/template-ish string for use in an alert name.

    Non-alphanumeric runs are treated as separators, so a discover id like
    ``cpu_{{ instance }}`` keeps its variable name (``CpuInstance``) — more
    descriptive than dropping it, and less collision-prone. Exact
    uniqueness is still guaranteed by the dedup pass in
    :func:`generate_prometheus_rules`, since camel-casing alone can map two
    distinct ids (``a_b`` and ``a__b``) to the same token.
    """
    return "".join(part[:1].upper() + part[1:] for part in _NON_ALNUM_RE.split(text) if part)


# Default placeholder values. These are deliberately generic starting
# points, NOT opinions — every one carries a ``# TODO`` marker in the
# output so a reviewer can't mistake them for tuned values. ``for:`` has
# to be a real duration for the manifest to apply, hence a value rather
# than a bare ``TODO`` token.
_TODO_FOR = "5m"
_TODO_SEVERITY = "warning"


def _is_templated(query: QueryConfig) -> bool:
    """True when the query id is a discover template (no literal id)."""
    return "{{" in query.id


def _outside_threshold_alert(group: GroupConfig, query: QueryConfig, name: str) -> list[str]:
    """Render the per-query outside-threshold alert lines.

    ``name`` is the caller-resolved, de-duplicated alert name so the
    generated file is duplicate-rule-lint clean and each alert is
    individually routable / silenceable in Alertmanager.
    """
    lines = [
        f"        # Outside-threshold alert for query {query.id!r} in group {group.name!r}.",
    ]
    if _is_templated(query):
        # A discover-templated id has no single literal value, so scope by
        # group only and leave a marker for the operator to narrow it.
        selector = f'anomaly_outside_threshold{{group="{group.name}"}} == 1'
        lines.append(
            "        # NOTE: id is discover-templated; this fires for every expanded "
            'series. Narrow with an id=~"..." matcher once the expansions are known.'
        )
    else:
        selector = f'anomaly_outside_threshold{{group="{group.name}", id="{query.id}"}} == 1'
    lines.extend(
        [
            f"        - alert: {name}",
            f"          expr: {selector}",
            f"          for: {_TODO_FOR}  # TODO: set the flap-suppression window for your SLO",
            "          labels:",
            f"            severity: {_TODO_SEVERITY}  # TODO: set severity",
            "          annotations:",
            '            summary: "Anomaly above threshold for {{ $labels.id }} '
            '({{ $labels.group }})"',
            "            description: |  # TODO: tailor / add a runbook_url",
            "              Detector {{ $labels.detector }} reported anomaly_outside_threshold=1",
            f"              for at least {_TODO_FOR} in group {group.name}.",
        ]
    )
    return lines


def _ensemble_alert(group: GroupConfig, name: str) -> list[str]:
    """Render the per-group ensemble-agreement alert (when configured).

    ``name`` is the caller-resolved, de-duplicated alert name.
    """
    return [
        f"        # Ensemble agreement for group {group.name!r} "
        f"(ensemble: {group.ensemble.method} configured).",  # type: ignore[union-attr]
        f"        - alert: {name}",
        f'          expr: anomaly_composite_outside_threshold{{group="{group.name}"}} == 1',
        f"          for: {_TODO_FOR}  # TODO: set the flap-suppression window for your SLO",
        "          labels:",
        f"            severity: {_TODO_SEVERITY}  # TODO: set severity",
        "          annotations:",
        '            summary: "Ensemble agreement on anomaly for {{ $labels.id }} '
        '({{ $labels.group }})"',
        "            description: |",
        f"              Multiple detectors agree the signal is anomalous in group {group.name}.",
    ]


def _recording_rules() -> list[str]:
    """Render the global ``max``-ensemble recording-rule fallback.

    One rule covering every group: ``by (id, group)`` already partitions
    the output per series, so a single rule is correct (and avoids the
    duplicate-recording-rule lint that per-group copies would trip).
    """
    return [
        "        # max-ensemble OR rollup — the recording-rule equivalent of an",
        "        # `ensemble: { method: max }` block (see docs/patterns.md). Covers",
        "        # every group: `by (id, group)` partitions the output per series.",
        "        - record: anomaly_outside_threshold_any",
        "          expr: max by (id, group) (anomaly_outside_threshold)",
    ]


def _make_unique(base: str, used: set[str]) -> str:
    """Return ``base``, or ``base2``/``base3``/… if it's already taken.

    Guarantees globally-unique alert names even when two distinct query
    ids camel-case to the same token, keeping the manifest
    duplicate-rule-lint clean.
    """
    candidate = base
    suffix = 2
    while candidate in used:
        candidate = f"{base}{suffix}"
        suffix += 1
    used.add(candidate)
    return candidate


def generate_prometheus_rules(cfg: Config, *, name: str = "promanomaly-generated") -> str:
    """Build a PrometheusRule manifest scaffolding from a validated config.

    Returns the YAML document as a string. One alerting group per detector
    group (with one ``AnomalyOutsideThreshold`` alert per query, plus an
    ``AnomalyEnsembleAgreement`` alert when the group sets ``ensemble:``),
    and one recording group holding the global ``max``-ensemble fallback.
    """
    # Rule-group names must be unique within a file. Alert groups are
    # named ``promanomaly.<group>``; pick a recording-group name that can't
    # collide even with a detector group literally named ``recording``.
    alert_group_names = {f"promanomaly.{g.name}" for g in cfg.groups}
    recording_group = _make_unique("promanomaly.recording", set(alert_group_names))

    lines: list[str] = [
        "# Generated by `promanomaly generate-rules` — scaffolding, not opinions.",
        "#",
        "# Selectors (id / group) are filled in from your config. Severities and",
        "# `for:` windows are TODO placeholders: the project ships no opinionated",
        "# thresholds, so tune every `# TODO` below for your own SLOs before wiring",
        "# this into Alertmanager. Re-run after config changes and diff.",
        "apiVersion: monitoring.coreos.com/v1",
        "kind: PrometheusRule",
        "metadata:",
        f"  name: {name}",
        "  labels:",
        "    app.kubernetes.io/name: promanomaly",
        "    app.kubernetes.io/part-of: promanomaly",
        "    # TODO: add the label your Prometheus Operator selects on, e.g.",
        "    #   release: prometheus",
        "spec:",
        "  groups:",
        f"    - name: {recording_group}",
        "      rules:",
    ]
    lines.extend(_recording_rules())

    used_names: set[str] = set()
    for group in cfg.groups:
        lines.append(f"    - name: promanomaly.{group.name}")
        lines.append("      rules:")
        for query in group.queries:
            name = _make_unique(
                f"Anomaly{_camel(group.name)}{_camel(query.id)}OutsideThreshold", used_names
            )
            lines.extend(_outside_threshold_alert(group, query, name))
        if group.ensemble is not None:
            name = _make_unique(f"Anomaly{_camel(group.name)}EnsembleAgreement", used_names)
            lines.extend(_ensemble_alert(group, name))

    return "\n".join(lines) + "\n"


__all__ = ["generate_prometheus_rules"]
