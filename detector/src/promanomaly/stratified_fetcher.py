"""Sliding-window baseline cache + fetch for stratified detectors.

A class extracted from :class:`Runner` so the cache + fetch + prune
lifecycle isn't tangled with scoring, ensembling, and discovery
bookkeeping. The runner owns one :class:`StratifiedFetcher` and
delegates: ``await self._stratified.fetch(...)``, ``self._stratified.
prune_for_discovery(...)``, ``self._stratified.replace_config(...)``.

The cache is per-(group, rendered_id) and rebuilt from zero on
restart, matching the rest of the detector's stateless-across-restarts
contract. Discovery-driven churn is bounded two ways:

- ``safety.max_stratified_cache_entries`` — hard cap with LRU eviction
  by ``fetched_at``.
- ``safety.discovery.forget_after_runs`` — per-rendered-id miss counter
  that drops cache entries once a discovered id stops appearing.

The runner consumes the cache for scoring; it does not mutate cache
state directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
import structlog

from .config import Config, DetectorEntry, GroupConfig, QueryConfig
from .detectors import get
from .source import PromQLSource, QueryResult, QuerySeries, SourceQueryError
from .stratified import baseline_refresh_seconds_for, query_window_seconds_for

logger = structlog.get_logger(__name__)


@dataclass
class _StratifiedBaseline:
    """Cached long-history fetch for one (group, query) under the
    sliding-window strategy.

    The runner caches the heavy multi-week baseline pull and only
    refreshes it at ``baseline_refresh_interval`` cadence; every detector
    run still pulls the short window fresh and stitches it onto this
    cache before scoring. Keyed by (group, rendered_id); rebuilt from
    zero on restart and on config reload (state for removed groups is
    dropped by :meth:`StratifiedFetcher.replace_config`).

    ``promql`` is captured so a reload that changes the PromQL string of
    a kept ``(group, query_id)`` invalidates the cache — otherwise the
    fetcher would silently stitch the old signal's baseline onto the new
    signal's short window.

    ``template_query_id`` and ``template_promql`` carry the
    pre-substitution form for discover-templated queries (and equal the
    rendered form for non-discover queries). Reload identity is checked
    against the template, not the rendered form, so reloading a
    discover template's PromQL invalidates all its rendered descendants
    at once — and reloading without any change preserves them.
    """

    fetched_at: float
    promql: str
    window_seconds: float
    step_seconds: float
    series_by_key: dict[tuple[tuple[str, str], ...], QuerySeries]
    template_query_id: str = ""
    template_promql: str = ""
    truncated: bool = False


def _stitch_series(base: QuerySeries | None, fresh: QuerySeries) -> QuerySeries:
    """Combine a cached baseline window with the latest short window.

    Cached baseline samples whose timestamp is at or after the earliest
    timestamp in the fresh window are dropped — the fresh fetch is the
    source of truth for the overlap. New series (no cached baseline)
    fall back to the short window unchanged; the detector then either
    handles the small slice or surfaces ``insufficient_data`` per its
    own contract.
    """
    if base is None or base.samples.empty:
        return fresh
    if fresh.samples.empty:
        return base
    earliest_fresh_ts = float(fresh.samples["timestamp"].iloc[0])
    base_keep = base.samples[base.samples["timestamp"] < earliest_fresh_ts]
    if base_keep.empty:
        combined = fresh.samples.reset_index(drop=True)
    else:
        combined = pd.concat([base_keep, fresh.samples], ignore_index=True)
    return QuerySeries(labels=dict(fresh.labels), samples=combined)


def _query_has_stratified_detector(entry: DetectorEntry) -> bool:
    """Best-effort: does this detector entry opt into the sliding-window strategy?

    Resolves the class via the registry; an unknown name resolves to
    False so a misconfigured detector doesn't keep a stratified cache
    entry alive across reload. The runner's unknown-detector handling
    surfaces the bad reference at run time anyway.
    """
    try:
        cls = get(entry.name)
    except Exception:
        return False
    merged: dict[str, Any] = dict(getattr(cls, "defaults", {}) or {})
    merged.update(entry.params or {})
    return (
        query_window_seconds_for(cls, merged) is not None
        and baseline_refresh_seconds_for(cls, merged) is not None
    )


class StratifiedFetcher:
    """Owns the stratified baseline cache + the sliding-window fetch.

    One instance per :class:`Runner`. Acts as a thin facade between the
    runner's scoring loop and the heavy per-query multi-week pull: the
    runner asks for ``fetch()``, this class either re-pulls or serves
    the cached baseline stitched with a fresh short window. Discovery
    churn and reload-driven invalidation are this class's problem, not
    the runner's.
    """

    def __init__(self, *, source: PromQLSource, config: Config) -> None:
        self._source = source
        self._config = config
        # Stratified-detector baseline cache. Keyed by (group, query_id)
        # — the heavy multi-week pull lives here and is reused for
        # ``baseline_refresh_interval`` before the next fetch. Entries
        # for removed groups / queries are dropped by ``replace_config``
        # so a reload that trims the config does not leak stale baselines.
        self._cache: dict[tuple[str, str], _StratifiedBaseline] = {}
        # Per (group, query.id template) -> dict mapping
        # ``rendered_id`` -> consecutive runs absent. Used to evict
        # stratified baseline cache entries for ``discover:``-templated
        # queries whose rendered ids stop appearing. Ages out on the
        # same ``safety.discovery.forget_after_runs`` threshold the
        # snapshot store uses for signal-missing tracking.
        self._discovery_misses: dict[tuple[str, str], dict[str, int]] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def replace_source(self, new_source: PromQLSource) -> None:
        """Swap the underlying PromQL source (e.g. on datasource reload)."""
        self._source = new_source

    def replace_config(self, new_config: Config) -> None:
        """Update the config and prune state the new shape no longer owns."""
        self._config = new_config
        self._prune_on_reload(new_config)
        new_template_keys: set[tuple[str, str]] = {
            (group.name, query.id)
            for group in new_config.groups
            for query in group.queries
            if query.discover
        }
        for tracker_key in list(self._discovery_misses):
            if tracker_key not in new_template_keys:
                del self._discovery_misses[tracker_key]

    # ------------------------------------------------------------------
    # Test ergonomics — kept as plain methods so the runner can delegate
    # without copying signatures.
    # ------------------------------------------------------------------
    def cache_keys(self) -> set[tuple[str, str]]:
        return set(self._cache.keys())

    def cache_size(self) -> int:
        return len(self._cache)

    def discovery_misses_copy(self) -> dict[tuple[str, str], dict[str, int]]:
        return {key: dict(inner) for key, inner in self._discovery_misses.items()}

    def age_cache_entry(self, cache_key: tuple[str, str], delta_seconds: float) -> None:
        """Backdate one cache entry's ``fetched_at`` for staleness tests."""
        self._cache[cache_key].fetched_at -= delta_seconds

    # ------------------------------------------------------------------
    # Fetch + cache invalidation
    # ------------------------------------------------------------------
    async def fetch(
        self,
        *,
        group: GroupConfig,
        query: QueryConfig,
        end: float,
        short_window_seconds: float,
        baseline_window_seconds: float,
        baseline_refresh_seconds: float,
        template_query: QueryConfig | None = None,
        source: PromQLSource | None = None,
    ) -> QueryResult:
        """Sliding-window fetch for queries with a stratified detector.

        Pulls the short window every refresh and the long baseline
        window only when the per-(group, query) cache is stale (or the
        baseline window has grown via a reload). The two results are
        stitched per-series — cached baseline samples before the earliest
        timestamp in the short window, then the fresh short window —
        before being handed back to the scoring path.

        TSDB-failure semantics: a baseline fetch raising
        :class:`SourceQueryError` propagates so the surrounding
        ``safety.on_source_failure`` policy applies; a stale-but-present
        cache is not silently reused. A short-window failure likewise
        propagates. The bookkeeping is done here so detectors never see
        partial state.

        ``template_query`` (when set, i.e. for discover-templated
        variants) carries the pre-substitution id + promql. The cache
        records those so reload-time identity checks compare templates
        rather than rendered ids — without that, every reload would
        wipe every discover-derived cache entry (the template id never
        equals the rendered id).
        """
        effective_source = source or self._source
        defaults = self._config.defaults
        max_series = self._config.safety.max_series_per_query
        cache_key = (group.name, query.id)
        template = template_query or query

        try:
            short_result = await effective_source.range_query(
                promql=query.promql,
                end=end,
                window_seconds=short_window_seconds,
                step_seconds=defaults.step_seconds,
                max_series=max_series,
            )
        except SourceQueryError as exc:
            logger.warning(
                "stratified_baseline_fetch_failed",
                group=group.name,
                query=query.id,
                phase="short",
                reason=exc.reason,
                detail=exc.message,
                cache_present=cache_key in self._cache,
            )
            raise

        cached = self._cache.get(cache_key)
        baseline_stale = (
            cached is None
            or (end - cached.fetched_at) >= baseline_refresh_seconds
            or cached.window_seconds != baseline_window_seconds
            or cached.step_seconds != defaults.step_seconds
            # Defensive: if a caller swapped the config in-place (rather
            # than through replace_config), the cache's recorded promql
            # may no longer match.
            or cached.promql != query.promql
        )
        if baseline_stale:
            try:
                baseline_result = await effective_source.range_query(
                    promql=query.promql,
                    end=end,
                    window_seconds=baseline_window_seconds,
                    step_seconds=defaults.step_seconds,
                    max_series=max_series,
                )
            except SourceQueryError as exc:
                logger.warning(
                    "stratified_baseline_fetch_failed",
                    group=group.name,
                    query=query.id,
                    phase="baseline",
                    reason=exc.reason,
                    detail=exc.message,
                    cache_present=cached is not None,
                )
                raise
            cached = _StratifiedBaseline(
                fetched_at=end,
                promql=query.promql,
                window_seconds=baseline_window_seconds,
                step_seconds=defaults.step_seconds,
                series_by_key={s.label_key: s for s in baseline_result.series},
                template_query_id=template.id,
                template_promql=template.promql,
                truncated=baseline_result.truncated,
            )
            self._cache[cache_key] = cached
            self._evict_to_cap()

        assert cached is not None  # established by either branch above
        stitched: list[QuerySeries] = []
        for series in short_result.series:
            base = cached.series_by_key.get(series.label_key)
            stitched.append(_stitch_series(base, series))

        return QueryResult(
            series=stitched,
            truncated=short_result.truncated or cached.truncated,
        )

    def prune_for_discovery(
        self,
        *,
        group: GroupConfig,
        query: QueryConfig,
        active_rendered_ids: set[str],
    ) -> None:
        """Age out stratified baseline cache entries for vanished discover ids.

        Mirrors the snapshot store's ``forget_after_runs`` semantics for
        the signal-missing tracker, so both state-bounding paths age out
        on the same threshold. Currently-active rendered ids reset the
        miss counter; absent ones increment it and get evicted from the
        cache (and the tracker) once they cross
        ``safety.discovery.forget_after_runs`` consecutive absent runs.
        """
        forget_after = self._config.safety.discovery.forget_after_runs
        tracker_key = (group.name, query.id)
        misses = self._discovery_misses.setdefault(tracker_key, {})

        for rid in active_rendered_ids:
            misses[rid] = 0

        for rid in list(misses):
            if rid in active_rendered_ids:
                continue
            misses[rid] += 1
            if misses[rid] > forget_after:
                del misses[rid]
                self._cache.pop((group.name, rid), None)

    # ------------------------------------------------------------------
    # Cache invariants
    # ------------------------------------------------------------------
    def _evict_to_cap(self) -> None:
        """Enforce ``safety.max_stratified_cache_entries`` via LRU eviction."""
        cap = self._config.safety.max_stratified_cache_entries
        overflow = len(self._cache) - cap
        if overflow <= 0:
            return
        # ``fetched_at`` is wall clock; older entries lose. ``sorted`` is
        # O(n log n) but the cache only grows past the cap when discovery
        # actively churns — most groups never hit this path.
        keys_by_age = sorted(self._cache.items(), key=lambda kv: kv[1].fetched_at)
        for cache_key, _ in keys_by_age[:overflow]:
            del self._cache[cache_key]
        logger.warning(
            "stratified_cache_overflow_evicted",
            evicted=overflow,
            cap=cap,
            note=(
                "stratified baseline cache exceeded max_stratified_cache_entries; "
                "oldest entries dropped to fit"
            ),
        )

    def _prune_on_reload(self, new_config: Config) -> None:
        """Evict stratified cache entries the new config no longer owns.

        Three eviction conditions, all checked against the *template*
        identity (id + promql before any discover: substitution) so a
        reload's effect on a discover query's many rendered descendants
        is uniform:

        - Template id absent from the new config (group removed or
          query renamed / deleted).
        - Template promql changed under the reload — would otherwise
          stitch the old signal's baseline onto the new signal's short
          fetch.
        - Template still present but no longer has a stratified
          detector — the cache entry is dead weight (``fetch`` will
          never read it).
        """
        live_templates: dict[tuple[str, str], str] = {}
        for group in new_config.groups:
            for query in group.queries:
                has_stratified = any(
                    _query_has_stratified_detector(entry) for entry in query.detectors
                )
                if has_stratified:
                    live_templates[(group.name, query.id)] = query.promql

        for cache_key in list(self._cache):
            cached = self._cache[cache_key]
            template_id = cached.template_query_id or cache_key[1]
            template_promql = cached.template_promql or cached.promql
            live_template_promql = live_templates.get((cache_key[0], template_id))
            if live_template_promql is None or live_template_promql != template_promql:
                del self._cache[cache_key]


__all__ = [
    "StratifiedFetcher",
    "_StratifiedBaseline",
    "_query_has_stratified_detector",
    "_stitch_series",
]
