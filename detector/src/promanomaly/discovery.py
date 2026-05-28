"""Dynamic series discovery via PromQL templating.

Each :class:`promanomaly.config.QueryConfig` may declare one or more
``discover:`` variables. For each variable the detector runs a probe
PromQL, harvests the distinct values of the named label, and expands
the query body and id template across the Cartesian product of all
variables. The expanded queries flow through the normal per-query
detection pipeline.

Discovery is a pure expansion step: it never scores anything itself.
The output of :func:`expand_query` is a list of :class:`ExpandedQuery`
plus the labels carried by each rendered variant, so the runner can
both attach those labels to ``anomaly_signal_missing`` for the absence
case (v0.4 dynamic counterpart to the v0.1 static ``expect:`` flag)
and feed the rendered PromQL into the source layer unchanged.
"""

from __future__ import annotations

import itertools
import re
import string as _string
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .config import DiscoverVariable, QueryConfig
from .source import QueryResult, SourceQueryError

# Mustache-style placeholders. Same regex shared with the runner.
_TEMPLATE_RE = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")

# Prometheus name validator — applied to the rendered query id since the
# config-time validator deliberately skipped templates. Matches the
# regex used in ``config._validate_prom_name``.
_PROM_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class DiscoveryError(RuntimeError):
    """Raised when discovery cannot produce any usable expansion.

    Includes a ``reason`` slug suitable for bounded-cardinality use as a
    Prometheus label value on ``anomaly_discovery_failures_total``.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(f"{reason}: {message}")
        self.reason = reason
        self.message = message


# Positive allowlist for characters that may be substituted verbatim
# into a PromQL template. Our naive substitution does no escaping, so
# anything outside this set is either a PromQL meta-character (``"``,
# ``\``, whitespace) or an unusual code point (Unicode bidi marks,
# combining diacritics, control characters) that could break quoting
# in non-obvious ways. Discovery targeting a label that genuinely
# contains such characters is almost always an operator mistake; we
# reject at probe time rather than silently filtering or escaping.
#
# The allowlist matches what operators conventionally use for instance
# / job / cluster labels: ASCII alphanumerics + a small set of
# punctuation common in DNS / k8s identifiers. Empty strings are also
# rejected since they would expand into ``instance=""`` which matches
# everything.
_SAFE_VALUE_CHARS = frozenset(_string.ascii_letters + _string.digits + "-_.:/")


def is_safe_discovered_value(value: str) -> bool:
    """True iff ``value`` can be substituted verbatim into a PromQL template."""
    if not value:
        return False
    return all(ch in _SAFE_VALUE_CHARS for ch in value)


def render_template(template: str, bindings: dict[str, str]) -> str:
    """Substitute ``{{ var }}`` placeholders in ``template``.

    Exposed publicly so the runner can render an id for
    ``anomaly_signal_missing`` on missing discovered series using the
    same logic ``expand_query`` uses for present ones — keeping the
    ``id`` label value consistent across present/missing states for the
    same logical series.
    """
    return _render(template, bindings)


@dataclass(frozen=True)
class ExpandedQuery:
    """One rendered variant of a discover-templated query.

    ``labels`` carries the variable bindings the expansion used (e.g.
    ``{"instance": "node-17"}``); they become the labels on
    ``anomaly_signal_missing`` if the series later disappears.
    ``label_key`` is the hashable form used as a discovery-tracking key
    inside :class:`promanomaly.state.SnapshotStore`.
    """

    rendered_id: str
    rendered_promql: str
    labels: dict[str, str]

    @property
    def label_key(self) -> tuple[tuple[str, str], ...]:
        return tuple(sorted(self.labels.items()))


def expand_query(query: QueryConfig, value_sets: dict[str, list[str]]) -> list[ExpandedQuery]:
    """Render one templated query into the full Cartesian fan-out.

    ``value_sets`` maps each variable name to the distinct label values
    harvested by its probe. Variables that appear in the template but
    not in ``value_sets`` raise :class:`DiscoveryError` — the runner
    treats that as a per-variable failure and skips the query for this
    run.
    """
    if not query.discover:
        return [
            ExpandedQuery(
                rendered_id=query.id,
                rendered_promql=query.promql,
                labels={},
            )
        ]
    # Validate that every template placeholder has a known variable.
    template_vars = _placeholders_in(query.id) | _placeholders_in(query.promql)
    declared = {d.variable for d in query.discover}
    missing = template_vars - declared
    if missing:
        raise DiscoveryError(
            "undeclared_variable",
            f"placeholders {sorted(missing)!r} have no matching discover: entry",
        )
    extra = declared - template_vars
    if extra:
        # Tolerated, but the operator probably mis-spelled a
        # placeholder; surface it once at expansion time.
        # A discover entry that is never referenced is a config smell.
        # Don't raise — keep the run going so a stray entry doesn't
        # take production offline mid-rollout — but the warning surfaces
        # in tests and logs.
        pass
    ordered_vars = [d.variable for d in query.discover]
    bindings_list: list[list[str]] = []
    for name in ordered_vars:
        values = value_sets.get(name, [])
        if not values:
            return []
        bindings_list.append(values)
    out: list[ExpandedQuery] = []
    for combo in itertools.product(*bindings_list):
        binding = dict(zip(ordered_vars, combo, strict=False))
        rendered_id = _render(query.id, binding)
        if not _PROM_NAME_RE.match(rendered_id):
            raise DiscoveryError(
                "invalid_rendered_id",
                f"rendered id {rendered_id!r} is not a valid Prometheus metric name",
            )
        rendered_promql = _render(query.promql, binding)
        out.append(
            ExpandedQuery(
                rendered_id=rendered_id,
                rendered_promql=rendered_promql,
                labels=binding,
            )
        )
    return out


def _placeholders_in(text: str) -> set[str]:
    return set(_TEMPLATE_RE.findall(text))


def _render(template: str, bindings: dict[str, str]) -> str:
    def _sub(match: re.Match[str]) -> str:
        name = match.group(1)
        return bindings.get(name, match.group(0))

    return _TEMPLATE_RE.sub(_sub, template)


async def probe_variable(
    variable: DiscoverVariable,
    *,
    probe_fn: Callable[[str], Awaitable[QueryResult]],
) -> tuple[list[str], list[str]]:
    """Execute the probe PromQL and return (safe_values, dropped_values).

    Returned values are sorted, deduplicated, and free of characters that
    would break naive substitution into the PromQL template (``"``, ``\\``,
    newlines). ``dropped_values`` carries the rejected ones so the runner
    can log + bump ``anomaly_discovery_failures_total{reason="unsafe_value"}``
    without losing the diagnostic.

    ``probe_fn`` is plumbed in so tests can pass a fake. The caller is
    responsible for catching :class:`DiscoveryError` (raised on source
    failure) and reporting the failure via
    ``anomaly_discovery_failures_total``.
    """
    try:
        result = await probe_fn(variable.probe)
    except SourceQueryError as exc:
        # Preserve the original TSDB reason (``timeout``, ``server_error``,
        # …) so the counter the runner ultimately bumps shares a bucket
        # with regular-query source failures. The runner re-raises this
        # to honour ``safety.on_source_failure`` and tags
        # ``anomaly_discovery_failures_total{reason="probe_query_failed"}``
        # separately for the discovery-specific dashboard.
        raise DiscoveryError(exc.reason, exc.message) from exc
    values: list[str] = []
    dropped: list[str] = []
    seen: set[str] = set()
    for series in result.series:
        value = series.labels.get(variable.label)
        if value is None or value in seen:
            continue
        seen.add(value)
        if not is_safe_discovered_value(value):
            dropped.append(value)
            continue
        values.append(value)
    return sorted(values), sorted(dropped)


__all__ = [
    "DiscoveryError",
    "ExpandedQuery",
    "expand_query",
    "is_safe_discovered_value",
    "probe_variable",
    "render_template",
]
