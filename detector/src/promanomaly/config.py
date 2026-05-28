"""YAML configuration schema for promanomaly.

The schema covers the current surface (datasource, server, safety, defaults,
groups+queries+detectors) and is intentionally tolerant of forward-looking
fields documented in the project guide (e.g. ``emit_baseline_stability``) so
configs that ship later can be parsed today without also being interpreted.
Forward fields default to safe no-ops; behaviour implemented later just
starts honouring them.
"""

from __future__ import annotations

import re
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

# Graduated from ``v1alpha1`` to ``v1`` in the v1.0 release. The previous
# alpha alias remains valid (byte-identical schema) but loads with a
# ``DeprecationWarning`` so operators get a nudge to bump their configs
# without their pods CrashLoopBackOff'ing on the next image pull. A future
# breaking change ships under a fresh apiVersion (``v2alpha1`` → ``v2``);
# v1 stays the long-term stable surface for the entire v1.x line.
CURRENT_API_VERSION = "promanomaly.io/v1"
DEPRECATED_API_VERSIONS = frozenset({"promanomaly.io/v1alpha1"})
SUPPORTED_API_VERSIONS = frozenset({CURRENT_API_VERSION}) | DEPRECATED_API_VERSIONS

# Prometheus metric/label name. ``id`` and ``group`` are emitted as label
# values but must additionally pass the same character rules as metric names
# and never contain colons (reserved for recording rules).
_PROM_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_prom_name(value: str, *, field: str) -> str:
    if ":" in value:
        raise ValueError(
            f"{field}={value!r}: colons are reserved for recording rules and "
            "are not allowed in promanomaly identifiers"
        )
    if not _PROM_NAME_RE.match(value):
        raise ValueError(f"{field}={value!r}: must match {_PROM_NAME_RE.pattern}")
    return value


# ``w`` (weeks) is supported alongside Prometheus's ms/s/m/h/d because
# stratified detectors with multi-week lookbacks (DayOfWeekMAD's
# ``lookback: 4w``) are unwieldy as ``28d``. We use lowercase ``w`` only;
# capital ``M`` (months) is deliberately omitted because month length is
# ambiguous and would mislead operators sizing buffers.
_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)\s*$")
_DURATION_UNITS = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
    "w": 7 * 86400.0,
}


def parse_duration(value: str | int | float) -> float:
    """Parse ``"5m"`` / ``"30s"`` / ``"1h"`` / ``"4w"`` style strings to seconds.

    Plain numbers are interpreted as seconds (caller convenience for
    config files that already use floats).
    """
    if isinstance(value, int | float):
        return float(value)
    match = _DURATION_RE.match(str(value))
    if not match:
        raise ValueError(f"invalid duration {value!r}; expected e.g. '30s', '5m', '1h', '4w'")
    return float(match.group(1)) * _DURATION_UNITS[match.group(2)]


def parse_timestamp(value: str, *, now: float) -> float:
    """Parse a CLI timestamp into unix seconds.

    Accepts RFC3339 (with optional ``Z`` suffix), unix seconds as a
    plain number, ``"now"``, or a relative ``-7d`` / ``-3h`` offset
    from ``now``. Used by ``cli analyze``, ``cli calibrate-buckets``,
    and ``backtest`` so all three share one mental model for time
    inputs.
    """
    import datetime as _dt

    value = value.strip()
    if value == "now":
        return float(now)
    if value.startswith("-"):
        # Relative offset from ``now`` — the caller chooses what ``now``
        # means (typically the already-parsed ``--to`` when resolving
        # ``--from``).
        return float(now) - parse_duration(value[1:])
    # Try unix seconds first (integer or float).
    try:
        return float(value)
    except ValueError:
        pass
    # RFC3339; ``fromisoformat`` handles the common ``Z`` suffix natively
    # on Python 3.11+.
    try:
        dt = _dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"invalid timestamp {value!r}; expected RFC3339, unix seconds, 'now', or '-7d'"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.UTC)
    return dt.timestamp()


Duration = Annotated[str, StringConstraints(strip_whitespace=True, min_length=2)]
PromName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class _ModelBase(BaseModel):
    """Common pydantic settings. ``extra='allow'`` lets unknown forward-looking
    fields ride through validation without dropping data — they re-serialise
    correctly even though the current runtime ignores them. Strict mode
    would bind us to the exact current schema, which makes the dev-config
    forward-compatibility story painful."""

    model_config = ConfigDict(extra="allow", str_strip_whitespace=True)


class AuthConfig(_ModelBase):
    type: Literal["none", "bearer", "basic", "mtls"] = "none"
    token: str | None = None
    username: str | None = None
    password: str | None = None
    ca_file: str | None = None
    cert_file: str | None = None
    key_file: str | None = None


class DatasourceConfig(_ModelBase):
    url: str
    timeout: Duration = "10s"
    auth: AuthConfig = Field(default_factory=AuthConfig)

    @property
    def timeout_seconds(self) -> float:
        return parse_duration(self.timeout)


class ReloadAuthConfig(_ModelBase):
    type: Literal["none", "bearer", "mtls"] = "none"
    token: str | None = None
    ca_file: str | None = None


class ReloadConfig(_ModelBase):
    enabled: bool = True
    auth: ReloadAuthConfig = Field(default_factory=ReloadAuthConfig)


class ServerConfig(_ModelBase):
    listen: str = ":9092"
    refresh_interval: Duration = "1m"
    reload: ReloadConfig = Field(default_factory=ReloadConfig)
    ready_endpoint: bool = True

    @property
    def refresh_interval_seconds(self) -> float:
        return parse_duration(self.refresh_interval)


class RedisConfig(_ModelBase):
    """Connection settings for the Redis-backed cache(s).

    A single Redis instance backs both the query cache (when HA mode is
    on) and the shared snapshot cache. Keys are prefixed so the two
    coexist safely on the same database.
    """

    url: str = "redis://localhost:6379/0"
    # All keys this process writes are prefixed with ``{key_prefix}:``
    # so multiple deployments can share a Redis without colliding.
    key_prefix: str = "promanomaly"
    # Per-connection timeout. Kept tight so a slow Redis cannot block
    # the detector pipeline for longer than the source-query budget.
    timeout: Duration = "2s"

    @property
    def timeout_seconds(self) -> float:
        return parse_duration(self.timeout)


class QueryCacheConfig(_ModelBase):
    """LRU cache for overlapping PromQL responses.

    Enabled by default with a conservative TTL — same PromQL inside one
    run (multiple queries, ensemble fan-out) is collapsed into one TSDB
    request without changing semantics.

    ``backend: memory`` (the default) keeps entries in a process-local
    LRU. ``backend: redis`` plugs into the same call site but stores
    entries in the configured Redis (set ``safety.redis.url``) so a
    leader and its followers share a single TSDB hit per (promql,
    window) bucket — the HA-mode variant from v0.4.
    """

    enabled: bool = True
    backend: Literal["memory", "redis"] = "memory"
    max_entries: int = Field(default=1024, ge=1)
    # 30s default mirrors the typical scrape cadence — two queries inside
    # the same tick share a result, but the second tick still refetches.
    ttl: Duration = "30s"

    @property
    def ttl_seconds(self) -> float:
        return parse_duration(self.ttl)


class DiscoveryConfig(_ModelBase):
    """Tuning knobs for dynamic series discovery.

    The state-bounding default exists so a workload that churns through
    ephemeral identities (transient pod names, replica IDs that never
    recycle) doesn't grow the discovery-tracker dict without bound.
    """

    # After this many consecutive missing runs a discovered series is
    # forgotten — ``anomaly_signal_missing`` stops being emitted and
    # the next time the series reappears it counts as newly discovered.
    # Default is large enough that operators get plenty of time to
    # notice and act on the alert (~16 hours at a 1m refresh) while
    # still bounding the worst-case state size.
    forget_after_runs: int = Field(default=1000, ge=1)


class HighAvailabilityConfig(_ModelBase):
    """Lease-based leader election + Redis-backed snapshot sharing.

    Off by default: the MVP topology is ``replicas: 1`` with
    ``strategy: Recreate``. Operators opting into multi-replica
    deployments flip ``enabled: true`` and the leader election + Redis
    snapshot cache come online. Followers serve the leader's most
    recent snapshot from Redis without issuing source queries.
    """

    enabled: bool = False
    # Coordination Lease parameters. Defaults mirror controller-runtime
    # so operators running other controllers on the same cluster don't
    # need to remember a new set.
    lease_name: str = "promanomaly-leader"
    lease_namespace: str = "default"
    lease_duration: Duration = "15s"
    retry_period: Duration = "2s"
    # Unique identity used inside the Lease. Defaults to the pod name
    # via the ``POD_NAME`` env var when unset; the leader.py module
    # resolves this at boot.
    identity: str | None = None
    # Where the shared snapshot lives. Mandatory whenever enabled=True;
    # validated at load time.
    snapshot_ttl: Duration = "5m"

    @property
    def lease_duration_seconds(self) -> float:
        return parse_duration(self.lease_duration)

    @property
    def retry_period_seconds(self) -> float:
        return parse_duration(self.retry_period)

    @property
    def snapshot_ttl_seconds(self) -> float:
        return parse_duration(self.snapshot_ttl)


class SafetyConfig(_ModelBase):
    max_series_per_query: int = Field(default=1000, ge=1)
    max_total_series: int = Field(default=20000, ge=1)
    detect_timeout: Duration = "5s"
    on_source_failure: Literal["serve_stale", "drop_scores", "fail_ready"] = "serve_stale"
    # Only consulted in fail_ready mode: after this many consecutive
    # source-failed runs for a group, the group's readiness flag flips
    # to False (and /ready returns 503 if no other group is ready).
    # A single transient failure must not take the pod out of the
    # service, so the default mirrors expect_grace_runs.
    fail_ready_after: int = Field(default=3, ge=1)
    query_cache: QueryCacheConfig = Field(default_factory=QueryCacheConfig)
    # Shared Redis settings; consumed by the query cache when
    # ``query_cache.backend=redis`` and by the HA snapshot cache.
    redis: RedisConfig = Field(default_factory=RedisConfig)
    discovery: DiscoveryConfig = Field(default_factory=DiscoveryConfig)
    # Hard cap on the runner's stratified baseline cache. Each entry
    # holds a multi-week per-series fetch and is keyed by (group,
    # rendered_id); without a cap, discover templates that churn
    # ephemeral ids would grow the cache without bound. LRU eviction
    # by ``fetched_at`` kicks in when this limit is reached.
    max_stratified_cache_entries: int = Field(default=2000, ge=1)

    @property
    def detect_timeout_seconds(self) -> float:
        return parse_duration(self.detect_timeout)


class AlertThresholds(_ModelBase):
    score: float = 3.0


class DefaultsConfig(_ModelBase):
    window: Duration = "1h"
    step: Duration = "15s"
    min_points: int = Field(default=60, ge=1)
    warmup_policy: Literal["emit_warming_up", "suppress", "emit_with_flag"] = "emit_warming_up"
    min_abs_delta: float = 0.0
    min_relative_delta: float = 0.0
    alert_thresholds: AlertThresholds = Field(default_factory=AlertThresholds)
    emit_baseline: bool = True
    emit_change_points: bool = True
    emit_baseline_stability: bool = True
    emit_duration: bool = True

    @property
    def window_seconds(self) -> float:
        return parse_duration(self.window)

    @property
    def step_seconds(self) -> float:
        return parse_duration(self.step)


_CANONICAL_OUTPUT_LABELS: frozenset[str] = frozenset(
    {"id", "group", "detector", "detector_instance"}
)


class ExporterLabelsConfig(_ModelBase):
    """Source-label allow / drop filtering applied at the exporter boundary.

    The detector copies labels from the source query onto its output
    metrics; for regulated or privacy-sensitive environments the operator
    needs a way to prevent customer ids, internal SKUs, or other
    sensitive labels reaching the monitoring plane via the anomaly
    metrics.

    Semantics:
    - ``allow=[]`` (default) — every source label flows through.
    - ``allow=[...]`` — only listed source labels are emitted; others
      are stripped.
    - ``drop=[...]`` — listed labels are always stripped. Applied after
      ``allow`` so an explicit drop overrides an explicit allow.

    The canonical labels emitted by the detector itself (``id``,
    ``group``, ``detector``, ``detector_instance``) cannot be filtered;
    they are added after the filter runs. Listing one of them in ``drop``
    is rejected at config-load time so an operator who *thinks* they are
    stripping ``id`` finds out at validate time, not when they wonder
    why their PromQL joins still work.
    """

    allow: list[str] = Field(default_factory=list)
    drop: list[str] = Field(default_factory=list)

    @field_validator("drop")
    @classmethod
    def _reject_canonical_in_drop(cls, value: list[str]) -> list[str]:
        offending = sorted(set(value) & _CANONICAL_OUTPUT_LABELS)
        if offending:
            raise ValueError(
                f"exporter.output_labels.drop cannot strip canonical detector "
                f"labels {offending}; they are added after the filter and "
                "always emitted"
            )
        return value

    @cached_property
    def allow_set(self) -> frozenset[str]:
        """Pre-built ``frozenset`` view of :attr:`allow` for hot-path lookups.

        The label filter on the runner's per-series fan-out is called
        several times per series per refresh; rebuilding the ``set`` on
        each call shows up at 10k+ series. ``cached_property`` is cheap
        on a pydantic model because the model is reconstructed on
        reload (no stale cache).
        """
        return frozenset(self.allow)

    @cached_property
    def drop_set(self) -> frozenset[str]:
        """Pre-built ``frozenset`` view of :attr:`drop`; see :attr:`allow_set`."""
        return frozenset(self.drop)


class ExporterConfig(_ModelBase):
    output_labels: ExporterLabelsConfig = Field(default_factory=ExporterLabelsConfig)


class OTLPConfig(_ModelBase):
    """OpenTelemetry OTLP exporter settings.

    Off by default — ``endpoint`` left unset means no tracer is built and
    every ``Telemetry.span`` call is a zero-overhead no-op. Operators
    typically point ``endpoint`` at a collector running in the same
    cluster (e.g. ``otel-collector.observability.svc:4317``).
    """

    # gRPC endpoint of the OpenTelemetry collector. ``None`` (the
    # default) disables tracing entirely.
    endpoint: str | None = None
    # Use plaintext gRPC. Defaults to True because the collector is
    # usually reached through in-cluster Service DNS; flip to False
    # when the collector terminates TLS.
    insecure: bool = True
    # service.name resource attribute reported alongside every span;
    # operators commonly distinguish multiple deployments via this label.
    service_name: str = "promanomaly"


class TelemetryConfig(_ModelBase):
    """Optional tracing configuration.

    Currently only OTLP is supported; the project guide is explicit that
    we don't reinvent the ecosystem. Adding Jaeger / Zipkin / etc. as
    separate top-level blocks is straightforward when an operator
    actually needs them.
    """

    otlp: OTLPConfig | None = None


_DISCOVER_VARIABLE_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class DiscoverVariable(_ModelBase):
    """One axis of dynamic series discovery.

    The ``probe`` PromQL is executed at run time and the distinct values
    of ``label`` across its result vector become the expansion set for
    this variable. The variable is referenced in the query body and the
    metric id template via ``{{ variable }}``.
    """

    variable: str
    probe: str
    label: str

    @field_validator("variable")
    @classmethod
    def _validate_variable(cls, value: str) -> str:
        if not _DISCOVER_VARIABLE_RE.match(value):
            raise ValueError(
                f"discover.variable={value!r}: must match {_DISCOVER_VARIABLE_RE.pattern}"
            )
        return value

    @field_validator("label")
    @classmethod
    def _validate_label(cls, value: str) -> str:
        if ":" in value:
            raise ValueError(f"discover.label={value!r}: colons are reserved for recording rules")
        return value


class DetectorEntry(_ModelBase):
    name: PromName
    instance: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _validate_prom_name(value, field="detector.name")

    @model_validator(mode="before")
    @classmethod
    def _accept_string_shorthand(cls, value: Any) -> Any:
        # ``- MAD`` is the convenient form when no overrides are needed;
        # pydantic gets a plain string here and we promote it into the
        # canonical ``{name: ..., params: {}}`` dict. Lists / non-string
        # scalars fall through and pydantic produces a normal type error
        # against ``name``.
        if isinstance(value, str):
            return {"name": value}
        return value


class QueryConfig(_ModelBase):
    # Plain string allowed (and the historic norm); when a ``discover:``
    # block is set the id is treated as a template — ``{{ variable }}``
    # placeholders are substituted per expansion at runtime. The
    # rendered ids still go through the Prometheus-name validator there.
    id: str
    promql: str
    detectors: list[DetectorEntry]
    expect: bool = False
    expect_grace_runs: int = Field(default=3, ge=1)
    min_abs_delta: float | None = None
    min_relative_delta: float | None = None
    alert_thresholds: AlertThresholds | None = None
    # Dynamic series discovery (v0.4). Each entry probes the TSDB for
    # the distinct values of one label; the query's ``promql`` and ``id``
    # are expanded once per Cartesian combination across all variables.
    discover: list[DiscoverVariable] = Field(default_factory=list)

    @field_validator("id")
    @classmethod
    def _validate_id(cls, value: str) -> str:
        # When the id contains no template placeholders we hold it to
        # the Prometheus-name rules immediately. Templated ids are
        # validated after substitution in the runner.
        if "{{" not in value:
            return _validate_prom_name(value, field="query.id")
        return value

    @model_validator(mode="after")
    def _validate_template_vs_discover(self) -> QueryConfig:
        has_template = "{{" in self.id or "{{" in self.promql
        if has_template and not self.discover:
            raise ValueError(
                f"query {self.id!r}: '{{{{ variable }}}}' placeholders require "
                "a discover: block defining the variable(s)"
            )
        if self.discover:
            names = [d.variable for d in self.discover]
            if len(names) != len(set(names)):
                raise ValueError(f"query {self.id!r}: duplicate discover.variable names: {names}")
        return self

    @model_validator(mode="after")
    def _check_detector_instances(self) -> QueryConfig:
        # Two detectors with the same name must each declare a unique
        # ``instance:``; one un-instanced and one instanced is also a
        # collision because the un-instanced one has no disambiguator.
        by_name: dict[str, list[str | None]] = {}
        for det in self.detectors:
            by_name.setdefault(det.name, []).append(det.instance)
        for name, instances in by_name.items():
            if len(instances) <= 1:
                continue
            seen: set[str | None] = set()
            for inst in instances:
                if inst in seen or inst is None:
                    raise ValueError(
                        f"query {self.id!r}: detector {name!r} appears multiple "
                        "times; each entry must declare a unique 'instance:'"
                    )
                seen.add(inst)
        return self


class EnsembleConfig(_ModelBase):
    method: Literal["max", "voting"] = "max"
    min_detectors: int = Field(default=2, ge=1)


# ``auto_select`` accepts ``true`` (winner-only output), ``false`` (off,
# the default), or ``"explicit_plus_best"`` (emit every configured
# detector plus a marker label on the winner). The string form lets
# operators opt in to keeping all detector outputs visible for
# comparison while still benefiting from the winner labelling.
AutoSelect = bool | Literal["explicit_plus_best"]


class GroupConfig(_ModelBase):
    name: PromName
    priority: int = 1
    queries: list[QueryConfig]
    ensemble: EnsembleConfig | None = None
    auto_select: AutoSelect = False
    auto_select_interval: Duration = "24h"

    @property
    def auto_select_interval_seconds(self) -> float:
        return parse_duration(self.auto_select_interval)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _validate_prom_name(value, field="group.name")

    @field_validator("queries")
    @classmethod
    def _validate_queries(cls, queries: list[QueryConfig]) -> list[QueryConfig]:
        if not queries:
            raise ValueError("group requires at least one query")
        ids = [q.id for q in queries]
        if len(ids) != len(set(ids)):
            raise ValueError(f"duplicate query id within group: {ids}")
        return queries

    @model_validator(mode="after")
    def _validate_ensemble_reachable(self) -> GroupConfig:
        # ``voting`` with min_detectors larger than every query's
        # detector count is a silent never-fire footgun: the composite
        # gauge stays at 0 forever, so the user thinks the alert simply
        # never trips. Reject when NO query in the group can ever reach
        # the threshold; allow the mixed case (some queries fire, some
        # are deliberately degenerate) since that may be intentional.
        ens = self.ensemble
        if ens is None or ens.method != "voting":
            return self
        max_detectors = max((len(q.detectors) for q in self.queries), default=0)
        if ens.min_detectors > max_detectors:
            raise ValueError(
                f"group {self.name!r}: ensemble.min_detectors={ens.min_detectors} "
                f"exceeds the largest configured detector list "
                f"({max_detectors}); composite metric would never fire"
            )
        return self


class Config(_ModelBase):
    apiVersion: str
    datasource: DatasourceConfig
    server: ServerConfig = Field(default_factory=ServerConfig)
    safety: SafetyConfig = Field(default_factory=SafetyConfig)
    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)
    exporter: ExporterConfig = Field(default_factory=ExporterConfig)
    highAvailability: HighAvailabilityConfig = Field(default_factory=HighAvailabilityConfig)
    telemetry: TelemetryConfig = Field(default_factory=TelemetryConfig)
    groups: list[GroupConfig]

    @model_validator(mode="after")
    def _validate_ha_requires_redis(self) -> Config:
        # HA mode needs the Redis snapshot cache; without it followers
        # would have no way to read the leader's output. Fail loud at
        # boot rather than producing inconsistent /metrics across pods.
        if self.highAvailability.enabled and not self.safety.redis.url:
            raise ValueError("highAvailability.enabled=true requires safety.redis.url to be set")
        if self.safety.query_cache.backend == "redis" and not self.safety.redis.url:
            raise ValueError("safety.query_cache.backend=redis requires safety.redis.url to be set")
        return self

    @model_validator(mode="after")
    def _validate_discovery_forget_window(self) -> Config:
        # ``forget_after_runs`` must outlast the longest configured
        # ``expect_grace_runs`` of any discover-templated query —
        # otherwise the missing-series transition fires for one run
        # and the entry is dropped before the next scrape window
        # rolls around, hiding the alert from operators. Surfacing
        # this at boot is much friendlier than the silent failure
        # mode in production.
        forget = self.safety.discovery.forget_after_runs
        for group in self.groups:
            for query in group.queries:
                if not query.discover:
                    continue
                if forget < query.expect_grace_runs:
                    raise ValueError(
                        f"safety.discovery.forget_after_runs={forget} is below "
                        f"query {query.id!r}.expect_grace_runs={query.expect_grace_runs}; "
                        "discovered series would be forgotten before signal_missing "
                        "has a chance to alert. Raise forget_after_runs or lower "
                        "expect_grace_runs."
                    )
        return self

    @field_validator("apiVersion")
    @classmethod
    def _validate_api_version(cls, value: str) -> str:
        if value not in SUPPORTED_API_VERSIONS:
            supported = sorted(SUPPORTED_API_VERSIONS)
            raise ValueError(f"unsupported apiVersion {value!r}; expected one of {supported}")
        if value in DEPRECATED_API_VERSIONS:
            # Byte-identical schema; nudge operators toward ``v1`` so the
            # eventual ``v2`` deprecation window doesn't catch them holding
            # an alpha alias. ``DeprecationWarning`` here is caught by
            # pytest and visible under CLI ``-W default``; the structured
            # boot log line is re-emitted from ``serve()`` AFTER
            # ``configure_logging`` has wired up the JSON pipeline (see
            # ``main.serve``), so operators reading the normal log stream
            # see it as a regular JSON warning record.
            import warnings

            warnings.warn(
                f"apiVersion {value!r} is deprecated; bump configs to "
                f"{CURRENT_API_VERSION!r}. The schema is byte-identical.",
                DeprecationWarning,
                stacklevel=2,
            )
        return value

    @field_validator("groups")
    @classmethod
    def _validate_groups(cls, groups: list[GroupConfig]) -> list[GroupConfig]:
        if not groups:
            raise ValueError("at least one group is required")
        names = [g.name for g in groups]
        if len(names) != len(set(names)):
            raise ValueError(f"duplicate group name: {names}")
        return groups

    @property
    def aggressive_refresh(self) -> bool:
        # Operational guard: ``refresh_interval`` should be at least
        # 1/6 of the rolling ``window``. Below that the scheduler fires
        # six or more passes per window's worth of data and the rolling
        # baseline lags. The roadmap is explicit that this is a warning,
        # not a hard fail — main.py logs at boot when this returns True.
        floor = self.defaults.window_seconds / 6.0
        return self.server.refresh_interval_seconds < floor


def load_config(path: str | Path) -> Config:
    """Parse a YAML config file into a validated :class:`Config`."""
    raw = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"config {path}: top level must be a mapping")
    cfg = Config.model_validate(raw)
    _validate_detector_params(cfg)
    return cfg


def _validate_detector_params(cfg: Config) -> None:
    """Type-check user-supplied detector params against each detector's ParamSpec.

    Local import: the detectors registry depends on ``config.parse_duration``,
    so a top-level import would be circular. Done at config-load (not as a
    pydantic validator) so the lookup runs once per load rather than once
    per validated ``DetectorEntry`` instance.

    Unknown param keys pass through — the runner merges defaults silently
    and detectors are documented as ignoring unknown keys for forward
    compatibility. Only declared keys are type-checked.
    """
    from .detectors import UnknownDetectorError
    from .detectors.registry import get as get_detector

    for group in cfg.groups:
        for query in group.queries:
            for entry in query.detectors:
                try:
                    cls = get_detector(entry.name)
                except UnknownDetectorError:
                    # Unknown detector is surfaced at runtime via
                    # ``surface_misconfigurations``; not our concern here.
                    continue
                specs = {spec.name: spec for spec in getattr(cls, "param_spec", ()) or ()}
                for key, value in (entry.params or {}).items():
                    spec = specs.get(key)
                    if spec is None:
                        continue
                    try:
                        spec.validate(value)
                    except ValueError as exc:
                        raise ValueError(
                            f"group {group.name!r} query {query.id!r} "
                            f"detector {entry.name!r}: {exc}"
                        ) from exc


__all__ = [
    "CURRENT_API_VERSION",
    "DEPRECATED_API_VERSIONS",
    "SUPPORTED_API_VERSIONS",
    "AlertThresholds",
    "AuthConfig",
    "Config",
    "DatasourceConfig",
    "DefaultsConfig",
    "DetectorEntry",
    "DiscoverVariable",
    "DiscoveryConfig",
    "EnsembleConfig",
    "ExporterConfig",
    "ExporterLabelsConfig",
    "GroupConfig",
    "HighAvailabilityConfig",
    "OTLPConfig",
    "QueryCacheConfig",
    "QueryConfig",
    "RedisConfig",
    "ReloadAuthConfig",
    "ReloadConfig",
    "SafetyConfig",
    "ServerConfig",
    "TelemetryConfig",
    "load_config",
    "parse_duration",
]
