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

CURRENT_API_VERSION = "promanomaly.io/v1alpha1"
SUPPORTED_API_VERSIONS = frozenset({CURRENT_API_VERSION})

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


_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$")
_DURATION_UNITS = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
}


def parse_duration(value: str | int | float) -> float:
    """Parse ``"5m"`` / ``"30s"`` / ``"1h"`` style strings to seconds.

    Plain numbers are interpreted as seconds (caller convenience for
    config files that already use floats).
    """
    if isinstance(value, int | float):
        return float(value)
    match = _DURATION_RE.match(str(value))
    if not match:
        raise ValueError(f"invalid duration {value!r}; expected e.g. '30s', '5m', '1h'")
    return float(match.group(1)) * _DURATION_UNITS[match.group(2)]


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
    watch_configmap: bool = True
    auth: ReloadAuthConfig = Field(default_factory=ReloadAuthConfig)


class ServerConfig(_ModelBase):
    listen: str = ":9092"
    refresh_interval: Duration = "1m"
    reload: ReloadConfig = Field(default_factory=ReloadConfig)
    ready_endpoint: bool = True

    @property
    def refresh_interval_seconds(self) -> float:
        return parse_duration(self.refresh_interval)


class QueryCacheConfig(_ModelBase):
    """In-process LRU cache for overlapping PromQL responses.

    Enabled by default with a conservative TTL — same PromQL inside one
    run (multiple queries, ensemble fan-out) is collapsed into one TSDB
    request without changing semantics. The Redis-backed HA variant
    ships in v0.4 and slots into the same call site.
    """

    enabled: bool = True
    max_entries: int = Field(default=1024, ge=1)
    # 30s default mirrors the typical scrape cadence — two queries inside
    # the same tick share a result, but the second tick still refetches.
    ttl: Duration = "30s"

    @property
    def ttl_seconds(self) -> float:
        return parse_duration(self.ttl)


class SafetyConfig(_ModelBase):
    max_series_per_query: int = Field(default=1000, ge=1)
    max_total_series: int = Field(default=20000, ge=1)
    series_overflow: Literal["drop_lowest_priority", "reject", "sample"] = "drop_lowest_priority"
    detect_timeout: Duration = "5s"
    query_timeout: Duration = "10s"
    on_source_failure: Literal["serve_stale", "drop_scores", "fail_ready"] = "serve_stale"
    # Only consulted in fail_ready mode: after this many consecutive
    # source-failed runs for a group, the group's readiness flag flips
    # to False (and /ready returns 503 if no other group is ready).
    # A single transient failure must not take the pod out of the
    # service, so the default mirrors expect_grace_runs.
    fail_ready_after: int = Field(default=3, ge=1)
    query_cache: QueryCacheConfig = Field(default_factory=QueryCacheConfig)

    @property
    def detect_timeout_seconds(self) -> float:
        return parse_duration(self.detect_timeout)

    @property
    def query_timeout_seconds(self) -> float:
        return parse_duration(self.query_timeout)


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


class ExporterLabelsConfig(_ModelBase):
    allow: list[str] = Field(default_factory=list)
    drop: list[str] = Field(default_factory=list)


class ExporterConfig(_ModelBase):
    output_labels: ExporterLabelsConfig = Field(default_factory=ExporterLabelsConfig)


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
    id: PromName
    promql: str
    detectors: list[DetectorEntry]
    expect: bool = False
    expect_grace_runs: int = Field(default=3, ge=1)
    min_abs_delta: float | None = None
    min_relative_delta: float | None = None
    alert_thresholds: AlertThresholds | None = None

    @field_validator("id")
    @classmethod
    def _validate_id(cls, value: str) -> str:
        return _validate_prom_name(value, field="query.id")

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
    groups: list[GroupConfig]

    @field_validator("apiVersion")
    @classmethod
    def _validate_api_version(cls, value: str) -> str:
        if value not in SUPPORTED_API_VERSIONS:
            supported = sorted(SUPPORTED_API_VERSIONS)
            raise ValueError(f"unsupported apiVersion {value!r}; expected one of {supported}")
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

    @model_validator(mode="after")
    def _validate_refresh_window_ratio(self) -> Config:
        # Operational guard: ``refresh_interval`` should be at least
        # 1/6 of the rolling ``window``. Below that the scheduler fires
        # six or more passes per window's worth of data and the rolling
        # baseline lags behind. We do NOT reject here — the roadmap is
        # explicit that this is a warning, not a hard fail — so users
        # who know what they are doing (sub-second test runs, etc.) can
        # opt in. ``aggressive_refresh`` exposes the warning for the
        # runner to log at boot.
        return self

    @property
    def aggressive_refresh(self) -> bool:
        floor = self.defaults.window_seconds / 6.0
        return self.server.refresh_interval_seconds < floor


def load_config(path: str | Path) -> Config:
    """Parse a YAML config file into a validated :class:`Config`."""
    raw = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"config {path}: top level must be a mapping")
    return Config.model_validate(raw)


__all__ = [
    "CURRENT_API_VERSION",
    "SUPPORTED_API_VERSIONS",
    "AlertThresholds",
    "AuthConfig",
    "Config",
    "DatasourceConfig",
    "DefaultsConfig",
    "DetectorEntry",
    "EnsembleConfig",
    "ExporterConfig",
    "ExporterLabelsConfig",
    "GroupConfig",
    "QueryCacheConfig",
    "QueryConfig",
    "ReloadAuthConfig",
    "ReloadConfig",
    "SafetyConfig",
    "ServerConfig",
    "load_config",
    "parse_duration",
]
