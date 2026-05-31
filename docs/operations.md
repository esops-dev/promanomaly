# Operating the Detector

Practical production notes: warm-up behaviour, TSDB hiccups, readiness, cardinality limits, and failure handling.

## Warm-up Policy

Until the rolling window has `defaults.min_points` samples, the detector has no baseline yet.

```yaml
defaults:
  min_points: 60
  warmup_policy: emit_warming_up   # emit_warming_up | suppress | emit_with_flag
```

| Mode                  | What you see during warm-up                          |
|-----------------------|-------------------------------------------------------|
| `emit_warming_up` (default) | `anomaly_warming_up{...}=1`; score is suppressed     |
| `suppress`            | No output at all                                      |
| `emit_with_flag`      | `anomaly_warming_up=1` + `anomaly_score=0` with extra `warming_up="true"` label |

**Default recommendation**  
`emit_warming_up` — dashboards stay visible and you can alert on “still warming up after 2× refresh interval”.

Use `suppress` only if hundreds of series would make the dashboard too noisy.

## Source-Failure Handling

Controls what happens when Prometheus/VictoriaMetrics is slow, throttled, or down.

```yaml
safety:
  on_source_failure: serve_stale   # serve_stale | drop_scores | fail_ready
  fail_ready_after: 3
```

| Mode            | Behaviour on failure                                      | Best for |
|-----------------|-----------------------------------------------------------|----------|
| `serve_stale` (default) | Last good snapshot stays on `/metrics`                   | Most dashboards |
| `drop_scores`   | Failing series disappear (Prometheus marks them stale)   | Rate-of-change or ratio panels |
| `fail_ready`    | Same as `serve_stale` + `/ready` goes 503 after N failures | Multi-replica setups with load balancing |

**Observability**  
`anomaly_source_failure_total{group, reason}` and `anomaly_source_failure_streak{group}` are always published.

## Cardinality Safeguards

```yaml
safety:
  max_series_per_query: 1000
  max_total_series: 20000
  series_overflow: drop_lowest_priority   # drop_lowest_priority | reject | sample
```

- `max_series_per_query` trims the biggest query.
- `max_total_series` trims across the whole run.
- Dropped series bump `anomaly_series_dropped_total`.

Rule of thumb: one 1-CPU replica comfortably handles ~10k series at 1-minute refresh.

## Readiness (`/ready`)

- Returns 200 as soon as **at least one** group has completed its first successful run.
- Per-group status visible in `anomaly_group_ready{group}`.
- Use the reference alert `AnomalyStale` on `anomaly_last_run_timestamp_seconds`.

## Config Reload

`/-/reload` or `SIGHUP` reloads the YAML instantly and atomically.  
Validation failures leave the old config running.  
New groups start immediately.

## Failure Isolation

Everything is isolated as early as possible:

- One bad series → others continue  
- One bad query → other queries in the group continue  
- One bad detector on a series → other detectors still run  
- Whole-group TSDB failure → follows `on_source_failure` policy  

`anomaly_failures_total{group, detector, reason}` tracks every issue.

## Per-Series Timeout

`safety.detect_timeout: 5s` (default)  
A single series that takes too long is cancelled and counted as a failure. The rest of the queue continues.  

This prevents one runaway detector from starving everything else.

## Practical-Significance Floors

Statistical detectors will (correctly) flag tiny moves on low-volume signals — a `0.01 → 0.012` step on a sparse error counter is statistically significant under MAD but is operational noise. Use the floor knobs to suppress alerts that don't matter operationally:

```yaml
defaults:
  min_abs_delta: 0.0        # require at least this many original units of movement
  min_relative_delta: 0.0   # require at least this fraction of baseline (0.05 = 5%)
```

Per-query overrides (`min_abs_delta` / `min_relative_delta` directly on a query) take precedence.

| Setting                | Effect                                                                  |
|------------------------|-------------------------------------------------------------------------|
| Both at `0` (default)  | Statistical threshold alone decides `anomaly_outside_threshold`         |
| `min_abs_delta` set    | Movement of at least N units (in metric’s own units) is required        |
| `min_relative_delta`   | Movement of at least N% of baseline is required                         |
| Both set               | Both must be satisfied for the alert bit to fire (logical AND)          |

`anomaly_score` is **unaffected** — only the alerting bit is gated, so dashboards keep showing the raw statistical signal. This lets you tune alerting independently of visualisation.

**Worked example.** On a sparse error counter where baseline ≈ `0.01`:

```yaml
defaults:
  alert_thresholds:
    score: 3.0
  min_abs_delta: 0.05      # suppress moves below ~5x background
  min_relative_delta: 0.5  # suppress moves below 50% of baseline
```

A `0.01 → 0.012` move now stays at `outside=0`. A `0.01 → 0.08` move trips both floors and fires.

**Zero baseline.** When the rolling baseline is exactly `0` (sparse counters that haven't fired yet, zero-clamped gauges, freshly-warmed series), "fraction of baseline" is undefined. The detector substitutes `1.0` as the denominator, so `min_relative_delta` degenerates into an absolute-units floor in that case: a `min_relative_delta: 0.05` with `baseline=0` requires `|y - 0| >= 0.05`. This is the most operator-friendly behaviour — a literal divide-by-zero or always-pass would both produce surprising alert noise on sparse counters at startup.

Add an explicit `min_abs_delta` if you want a different floor on zero-baseline series; the two compose with AND, so the looser of the two effectively governs.

## Ensemble Fusion

When a query runs multiple detectors, set an `ensemble:` block on the group to publish a single composite verdict instead of writing recording rules to deduplicate:

```yaml
groups:
  - name: latency_alerts
    ensemble:
      method: voting      # max | voting
      min_detectors: 2    # required when method: voting
    queries:
      - id: p95_latency
        promql: histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))
        detectors:
          - name: MAD
          - name: Hampel
          - name: IQR
```

`anomaly_composite_score{id, group, <source labels>}` is the max raw per-detector score for the series (so the gauge is comparable to individual scores). `anomaly_composite_outside_threshold{id, group, <source labels>}` is `1` when:

| Method   | Fires when                                                  |
|----------|-------------------------------------------------------------|
| `max`    | **Any** detector’s `anomaly_outside_threshold` is `1`        |
| `voting` | **At least `min_detectors`** detectors agree                 |

The composite metrics carry no `detector` label by design — they span multiple detectors, and source-side labels round out the join key for joining against per-detector metrics.

`auto_select` and `ensemble:` compose: `auto_select` decides which detector’s score is *emitted*, but the ensemble fuser still sees every configured detector’s verdict so voting semantics match what the config declares.

Weighted voting, Bayesian fusion, and ML-trained combiners are explicitly out of scope. Operators who want them compose recording rules; everything else drifts toward stateful tuning.

## Change-Point Detectors and the Counter

Change-point detectors (BOCPD, CUSUM) emit a counter, not just a gauge:

```
anomaly_change_point_total{id, group, detector, <source labels>}
```

The counter increments by 1 on each *fresh* fire at the latest sample — no double-counting on overlapping rolling-window calls. Useful queries:

```promql
# Count change-points in the last 10 minutes.
increase(anomaly_change_point_total[10m])

# Fleet-wide change-point burst (alert when many services regress at once).
sum by (group) (rate(anomaly_change_point_total[1m]))
```

The counter resets to zero on process restart (consistent with the project's stateless promise) — Prometheus's counter-reset detection handles that natively.

Two operator-facing knobs:

```yaml
defaults:
  emit_change_points: true   # default; flip to false to suppress the counter wholesale
```

```yaml
detectors:
  - name: BOCPD
    params:
      threshold: 0.5   # per-detector override of alert_thresholds.score
```

Per-detector threshold is honoured in **both** the live-scoring path and the calibration cycle. Don't set the group's `alert_thresholds.score` to a BOCPD-friendly value (e.g. 0.5) — it would also gate the sigma-multiplier detectors in the same group. Use the per-detector override instead.

See [`change-points.md`](change-points.md) for the algorithm details and worked examples.

## Stratified Detectors

Stratified detectors (`HourOfDayMAD`, `DayOfWeekMAD`) compare the current sample against historical samples from the same time bucket and need multi-week lookbacks. promanomaly runs them through a sliding-window fetch strategy that caches the heavy long-history query and only re-pulls it at `baseline_refresh_interval` cadence (default `1h`), while the latest short window is fetched every `refresh_interval` and stitched onto the cached baseline before scoring.

Operationally relevant knobs:

- `baseline_refresh_interval` per detector — trade TSDB load for baseline freshness.
- Boot-time warnings (`stratified_baseline_refresh_invalid`, `stratified_baseline_refresh_below_refresh_interval`, `stratified_lookback_invalid`) — grep for these in logs after a config change.
- `validate --probe` reports the projected per-day TSDB query count for every stratified detector — gate CI on `--strict` to catch wildcard cost overruns.
- HA: only the leader fetches; followers serve the leader's shared snapshot. On failover the new leader pays a one-time full baseline fetch for every stratified query.
- Calibration / auto-select is a best-effort surface for stratified detectors — the synthetic-injection patterns contaminate same-bucket baselines, so prefer explicit detector selection and consider `defaults.emit_baseline_stability: false`.

Full operational rationale, multi-instance behaviour, backtest interactions, and TSDB-cost guidance live in [`scaling-stratified.md`](scaling-stratified.md).

## Query Caching

The detector ships an in-process LRU + TTL cache for PromQL responses. Two unrelated callers that issue the same query inside the TTL share one TSDB hit — useful when the same expression is configured under multiple `id`s, when an ensemble runs several detectors against the same source query, or when `refresh_interval` is shorter than the rolling `step` and consecutive ticks land in the same bucket.

```yaml
safety:
  query_cache:
    enabled: true       # default; flip to false to bypass caching entirely
    max_entries: 1024   # LRU capacity (default)
    ttl: 30s            # how long an entry stays valid (default)
```

`end` is quantised to `step_seconds` so two callers within one scrape bucket coalesce onto the same cache slot. Across detector ticks the bucket key advances, so a new TSDB hit happens at most once per bucket.

Hit/miss counters surface as `anomaly_query_cache_hits_total`, `anomaly_query_cache_misses_total`, `anomaly_query_cache_evictions_total`, `anomaly_query_cache_expirations_total`, and the current size as `anomaly_query_cache_entries`. Use the hit ratio to tune `max_entries` for your workload.

**Backends**

```yaml
safety:
  query_cache:
    backend: memory     # default: in-process LRU+TTL
    # backend: redis    # shared cache across the leader + followers
  redis:
    url: redis://promanomaly-redis-master:6379/0
    key_prefix: promanomaly
    timeout: 2s
```

- `memory` (default) — process-local LRU, one cache per replica. Works for single-replica deployments.
- `redis` — shared via the configured Redis. In HA mode this means leader and followers see the same cached PromQL responses, so a TSDB hit is paid at most once across the cluster per (promql, window, step, end-bucket) tuple. Same `anomaly_query_cache_*` metrics; the wire format doesn't change.

The Redis client is shared with the HA snapshot cache (see below) so there's only one connection pool per pod.

## High Availability

Single-replica is the default and supported topology. For multi-pod deployments, enable HA mode:

```yaml
highAvailability:
  enabled: true
  lease_name: promanomaly-leader
  lease_namespace: ""        # defaults to the release namespace
  lease_duration: 15s
  renew_deadline: 10s
  retry_period: 2s
  snapshot_ttl: 5m
safety:
  redis:
    url: redis://promanomaly-redis-master:6379/0
```

What changes when HA is on:

- **Leader election** uses the standard `coordination.k8s.io/v1` Lease API. Only the elected leader runs the detector pipeline; followers serve `/metrics` from the shared snapshot cache. The Helm chart auto-renders the matching Role + RoleBinding scoped to the configured `lease_name` — no cluster-wide RBAC.
- **Strategy** flips from `Recreate` to `RollingUpdate`. Failover is the documented path, so rolling pod restarts are safe.
- **Shared snapshot cache** lives in Redis under `{key_prefix}:snapshot:<group>`. Followers poll the cache at `refresh_interval / 2` and feed snapshots into their local store; followers serving `/metrics` see the same wire output as the leader (within one poll interval).
- **Per-replica identity** comes from the Downward API (`POD_NAME` env). The chart wires this automatically; override with `highAvailability.identity` only if you need a stable identity independent of the pod name.

Two new operational metrics:

- `anomaly_leader_elected{instance="<pod>"}` — `1` on the current leader, `0` everywhere else.
- `anomaly_leader_transitions_total{instance="<pod>"}` — bumps each time *this* replica acquires the Lease.

**Failover behaviour**

- A new leader fires an *immediate* full detection run for every group on acquisition. Followers see fresh snapshots within one poll interval (≤ `refresh_interval / 2`) rather than waiting a full scheduler tick.
- The previous leader observes Lease loss in its renewal loop and stops scheduling within `refresh_interval / 2`. In-flight detector runs are abandoned rather than allowed to complete — the new leader's immediate run reproduces them.
- `snapshot_ttl` (default `5m`) bounds how long a stale snapshot can survive a leader that died without losing the Lease. Set it well above `refresh_interval` so transient leader changes don't blank `/metrics`.

**Sizing.** Three replicas is the typical HA setup. Anything past that is leader-election overhead without benefit — only one replica runs detectors at any moment.

## Horizontal Scale-Out (active/active sharding)

HA mode is **active/passive**: leader election guarantees exactly one replica runs detection, which is correct but caps total throughput at a single process. When one process can no longer score the whole estate within a refresh interval, switch to **active/active scale-out**: `N` replicas each own a disjoint shard of the series and run concurrently.

```yaml
scaleOut:
  enabled: true
  shards: 3        # run 3 replicas, each scoring ~1/3 of the series
```

The two modes are **mutually exclusive** — `scaleOut.enabled` and `highAvailability.enabled` both `true` is rejected at boot. Pick by question:

| Question | Mode |
|----------|------|
| "I want redundancy; one process can keep up." | **HA** (active/passive) |
| "One process can't score everything within a refresh." | **Scale-out** (active/active) |

How it works:

- **Stable shard index.** The chart renders a **StatefulSet** of `shards` pods (`pod-0`…`pod-N-1`); each derives its shard index from the pod ordinal in `POD_NAME` — no per-pod config. (Pin `scaleOut.shard_index` only for non-StatefulSet topologies.)
- **Series partition.** Each replica computes a **rendezvous hash** (highest-random-weight) of every candidate series key and scores only the series that hash to its shard. The partition is disjoint and the union covers the whole estate. The hash is process-stable (BLAKE2b, not Python's salted `hash`), so all replicas agree on ownership.
- **Resharding is minimal.** Scaling `shards` up or down moves only ~`1/shards` of the series (the consistent-hashing property), and a newly-started pod runs its first detection pass *immediately* on boot, so a moved series is re-scored on acquisition rather than after a tick — mirroring the HA failover rule.
- **Scraping.** The regular `Service` selects every shard's pod, so the existing `ServiceMonitor` collects all shards; their `/metrics` union is the whole estate. A headless Service (`<name>-headless`) backs the StatefulSet's stable DNS.

Two operational metrics expose the partition:

- `anomaly_shard{instance="<pod>"}` — the shard index this replica owns.
- `anomaly_shard_series_count{shard="<n>"}` — series scored by that shard, so you can confirm the partition is even.

**`max_total_series` becomes a per-shard cap.** Each replica enforces `safety.max_total_series` against *its own* shard's series (the cap is applied after the shard filter), so the whole-estate ceiling is `max_total_series × shards`. Size it as `(expected total series / shards) × headroom`, not the whole estate — see the sizing table in the [production checklist](production-checklist.md).

**Set `max_series_per_query` above your largest single query's cardinality.** `max_series_per_query` is a *per-query fetch* ceiling applied at the TSDB, **before** sharding — every shard fetches the same (TSDB-ordered) first `max_series_per_query` series and then keeps its slice. So sharding distributes scoring *within* that ceiling; it cannot scale a single query beyond it. Worse, if a query *truncates* (you'll see `anomaly_series_dropped_total{reason="cardinality_overflow"}`), different shards may fetch different truncated subsets and the partition is no longer guaranteed complete. Keep `max_series_per_query` comfortably above any single query's true cardinality so no query truncates; scale the *estate* by adding more queries/series, not by relying on sharding to subset one oversized query.

**No per-shard redundancy.** Scale-out is throughput, not availability — there is no leader/follower within a shard. If a shard's pod is down, that shard's slice of the estate goes unscored until Kubernetes reschedules it (Prometheus staleness handles the gap), and that shard's owner-only signals (recurrence, `expect:`/discovery absence) pause with it. Layer a `PodDisruptionBudget` and sane resource requests on the StatefulSet; if you need both redundancy *and* horizontal scale, that's a future "leader-per-shard" direction, not this release.

**Scaling — change `scaleOut.shards`, not the replica count.** The chart ties the StatefulSet's `replicas` to `scaleOut.shards`, so resizing is a single Helm value change that moves both together. Do **not** `kubectl scale` the StatefulSet directly: dropping replicas below `shards` orphans a shard's series (no pod owns that index and those series go unscored), and adding replicas past `shards` makes the extra pod fail at boot (its ordinal is out of range). Keep replicas == shards.

**TSDB query load is not reduced by sharding.** PromQL returns all of a metric's series, so every shard still runs the *full* query and then discards the series it doesn't own — sharding scales detection *compute*, not query throughput. If the TSDB read volume is the constraint, enable a shared query cache (`safety.query_cache.backend: redis` + `safety.redis.url`) so the shards' identical fetches collapse onto one TSDB hit per cache bucket.

**Group-level rollups under sharding.** A few signals are whole-group aggregates rather than per-series, and are handled so the partition never multiplies a series:

- **`anomaly_density`** (fleet density rollup) is **suppressed** in sharded mode — each shard sees only its slice, so an in-process value would be a partial fraction. Compute true fleet density with a recording rule over the per-series `anomaly_outside_threshold` (the shard union is complete), per [patterns](patterns.md).
- **Recurrence** heatmaps and **`expect:` / discovery `anomaly_signal_missing`** are emitted by a single *owner shard* (chosen by hashing the group / series identity), so each appears exactly once across the fleet. Recurrence re-reads the complete history from the TSDB, so the single copy is correct.

**Limitation — cohort detectors.** Sharding partitions by series key, so a cohort's members hash to different shards and each shard's Cohort detector would see only a partial population. Run cohort groups in a separate unsharded deployment (or accept the per-shard comparison). The detector logs `cohort_with_scaleout` at boot when it detects this combination.

## Per-Group Refresh Intervals

`server.refresh_interval` sets the default cadence for every group. A group can override it so cheap fast-moving signals and expensive stratified/cohort groups coexist in one detector without a global compromise:

```yaml
server:
  refresh_interval: 1m       # default for groups without an override
groups:
  - name: error_rates
    refresh_interval: 30s     # score hot signals more often
    queries: [ ... ]
  - name: stratified_latency
    refresh_interval: 5m      # expensive multi-week baseline — refresh rarely
    queries: [ ... ]
```

- An unset group `refresh_interval` falls back to `server.refresh_interval` — default behaviour is unchanged.
- The per-group lock already prevents a group's runs from overlapping; each group runs on its own scheduler cadence.
- The `refresh_interval >= window/6` quality guard is enforced **per group** at boot — a too-tight override logs `aggressive_refresh_interval{group}` just like a too-tight server default.
- Stratified detectors' separate baseline-refresh cadence is independent and composes with the per-group short-window cadence.
- `validate --estimate-cost` accounts for per-group cadences — each group reports its `refresh_interval_seconds` and a `short_queries_per_minute` rate, so a too-aggressive interval shows up as outsized TSDB load before deploy.

## Priority-Aware Scheduling and Load Shedding

By default group scheduling is unbounded — every group runs on its cadence with no admission control. When the TSDB is occasionally slow or throttled, cap concurrency so the detector protects its most important groups first:

```yaml
safety:
  max_concurrent_groups: 4    # at most 4 groups run at once
groups:
  - name: critical_slos
    priority: 10              # admitted ahead of lower-priority groups under load
    queries: [ ... ]
```

Under contention the scheduler orders pending runs by `priority` (the same field that drives cardinality overflow) and, when a group can't get a slot within its refresh interval, **sheds** the run rather than queuing unboundedly. Shedding is visible, never silent:

- `anomaly_group_skipped_total{group, reason="backpressure"}` — no slot within the interval under `max_concurrent_groups`.
- `anomaly_group_skipped_total{group, reason="overrun"}` — the previous run was still in flight at the next tick.

A skip **delays** a score; it never **suppresses** a detected anomaly — the last good snapshot stays in place per `serve_stale`, and the `AnomalyGroupStarved` reference alert fires on sustained shedding. This is a *scheduling* concern, not a *silencing* one; see [degraded-modes](operations/degraded-modes.md).

The cap governs *scheduled ticks*. The immediate first run fired on boot, on HA leader-acquisition, and after a config reload runs every group once regardless, so `/metrics` is populated promptly even with a low cap; admission control kicks in from the first scheduled tick onward. (Reload is operator-triggered, so a reload of a many-group config briefly fans out all groups at once — size the TSDB for that burst, or stagger heavy groups across deployments.)

## Dynamic Series Discovery

Hand-writing one query per cohort member doesn't scale past a handful of services. `discover:` lets one templated query fan out across the distinct values of a label at runtime:

```yaml
groups:
  - name: per_instance_cpu
    queries:
      - id: cpu_busy_{{ instance }}
        promql: 'avg by (instance) (rate(node_cpu_seconds_total{instance="{{ instance }}"}[1m]))'
        expect_grace_runs: 3
        discover:
          - variable: instance
            probe: 'up{job="node"}'
            label: instance
        detectors:
          - name: MAD
```

How it works:

1. On every detector run, each `discover:` entry executes its `probe` PromQL.
2. The distinct values of `label` across the probe result become the variable's value set.
3. The query's `id` and `promql` are rendered once per Cartesian combination across all declared variables; each rendered variant flows through the standard per-query pipeline.
4. `anomaly_discovery_expansions{group, query}` tracks the per-run fan-out so dashboards can spot drift in the discovered set over time.
5. Probe failures bump `anomaly_discovery_failures_total{group, query, variable, reason}` and skip the query for that run. `reason` is bounded (`probe_query_failed`, `empty_probe`, `undeclared_variable`, `invalid_rendered_id`).

**Catch foot-guns at PR time.** `validate --probe` simulates discovery: it runs every probe, expands, and reports the projected total series count alongside `safety.max_total_series`. Under `--strict` the CLI exits non-zero when the simulated total exceeds the cap — drop it into a CI job to catch wildcard mistakes before they hit production.

```bash
promanomaly validate --config promanomaly.yaml --probe --strict
```

**Signal-absence for discovered series.** Once the detector has seen a discovery key (e.g. `instance=node-17`) the absence of that key in a later run is itself an anomaly — the dynamic counterpart of the static `expect:` flag. After `expect_grace_runs` consecutive runs without the key, `anomaly_signal_missing{id, group, <discovered_labels>}=1` is emitted (with the **rendered** id, so the label value matches the one the series carried when present) and `anomaly_signal_missing_total{group, reason="missing_grace_exhausted"}` bumps once on the transition. The metric clears the next time the series reappears.

The same `AnomalySignalMissing` reference alert covers both the static `expect:` and the dynamic-discovery case.

**Bounding the tracker.** Workloads that churn through ephemeral identities (transient pod names, replica IDs that never recycle) would otherwise grow the discovery tracker without end. `safety.discovery.forget_after_runs` (default `1000`) drops a series from the tracker after that many consecutive missing runs; the next time it reappears it counts as a fresh discovery. The schema validates `forget_after_runs >= max(query.expect_grace_runs)` at boot — setting it lower would silently hide `anomaly_signal_missing` from operators.

**Unsafe discovered values.** Values containing `"`, `\`, or whitespace control characters would break naive substitution into the PromQL template, so they are filtered before expansion. The runner bumps `anomaly_discovery_failures_total{reason="unsafe_value"}` and logs the offenders; `validate --probe` surfaces them in the per-variable JSON entry under `dropped_unsafe` so operators can fix the discovery axis (or relabel upstream) at PR time.

**Probe failure under serve_stale.** A probe that hits a TSDB error (timeout, server_error, …) honours `safety.on_source_failure` the same way a regular query failure does — under the default `serve_stale` the previous snapshot is preserved instead of being overwritten with empty data. The original TSDB reason flows into `anomaly_source_failure_total{reason}` so it shares a bucket with regular-query failures; the discovery-specific `anomaly_discovery_failures_total{reason="probe_query_failed"}` counter is bumped separately for the discovery dashboard.

**HA mode.** When `highAvailability.enabled: true`, the discovery miss-counter tracker lives in the shared Redis cache (under `{key_prefix}:discovery:<group>:<query>`) instead of leader-local memory, so a Lease failover no longer resets it: the newly-elected leader inherits each series' consecutive-miss count and a series that disappeared right before the handover still trips `anomaly_signal_missing` on schedule rather than restarting its `expect_grace_runs` window from zero. Single-replica deployments keep the in-memory tracker unchanged. The Redis state is bounded by the same `safety.discovery.forget_after_runs` cap as the in-memory tracker, and the entries carry a TTL (refreshed every run) so a fully-removed deployment's keys expire rather than leaking. A transient Redis outage degrades gracefully — that run records no new observations and emits no missing signal — rather than crashing the detection run.

## Validating Configs Before Deploy

`promanomaly validate --config <file>` runs schema validation only. Add `--probe` to additionally execute every query against the live datasource:

```bash
promanomaly validate --config promanomaly.yaml --probe --strict
```

Output: one JSON line per query (`status`, `series`, optional `truncated` / `empty` / `reason`), then a summary line with `total_series`, `errored`, and `empty_unexpected`. `--strict` exits non-zero when any query is empty (excluding `expect:true` queries — empty is the *expected* state for those) or errors. Suitable as a pre-deploy CI gate.

A `--datasource-url` override lets CI probe a staging TSDB without editing the file.

## Introspecting Installed Detectors

If your image bundles community detectors via the `promanomaly.detectors` entry point group, `/debug/detectors` (HTTP, JSON) and `promanomaly detectors list` (CLI) both surface what the running registry contains:

```bash
# Local registry (no running detector required):
promanomaly detectors list

# Remote (proxies /debug/detectors verbatim):
promanomaly detectors list --target http://promanomaly.observability:9092
```

The shipped NetworkPolicy denies ingress to `/debug/*` by default — same-namespace pods can hit it; everything else has to opt in.

## Inspecting One Series Live

`/debug/inspect?id=<metric_id>&labels=<sel>` re-fetches the rolling window from the TSDB right now, re-runs every configured detector for the matching `(id, label-set)` series, and returns the per-detector score, baseline, threshold, and (when calibrated) confidence + stability — the canonical answer to "why did this series score 4.7?". The CLI wraps the same endpoint:

```bash
promanomaly inspect --target http://promanomaly.observability:9092 \
  --id error_rate --labels 'instance=host-a'

# Raw response for piping into jq:
promanomaly inspect --target http://promanomaly.observability:9092 \
  --id error_rate --output json | jq '.series[0].detectors'
```

Because each call refetches, two consecutive calls can return slightly different scores if the TSDB picked up a new sample between them. That is the correct semantics — operators want "right now", not the cached snapshot from the last scheduled run. It also means `inspect` adds a small TSDB query per call; the NetworkPolicy keeps the endpoint inside the cluster, which is the intended trust boundary.

## Output-Label Filtering

Source-label allow / drop filtering runs at the exporter boundary. Use it when the source query naturally carries identifiers you don't want on the monitoring plane — customer ids, SKUs, user-supplied tags, anything caught by GDPR / SOC 2 / HIPAA.

```yaml
exporter:
  output_labels:
    # Optional: when non-empty, only these source labels are emitted.
    allow:
      - instance
      - region
    # Always stripped (takes precedence over allow).
    drop:
      - customer_id
      - sku
```

Semantics:

- The canonical detector labels — `id`, `group`, `detector`, `detector_instance` — are added **after** the filter and cannot be stripped. Listing one of them in `drop` is rejected at config-load time so a configuration mistake fails at validate-time rather than silently leaving the label in production.
- `allow` and `drop` operate on **source** labels (everything copied from the PromQL response). The canonical labels and any opt-in feature labels (`cohort_label`, `warming_up`) bypass the filter. (The auto-select winner is no longer a label — it is the standalone `anomaly_best_detector` info gauge.)
- `drop` takes precedence over `allow` — if a label appears in both, it is stripped. Useful when the allow-list is an inherited template and a downstream config layer needs to subtract.
- The filter applies uniformly to `anomaly_score`, `anomaly_baseline`, `anomaly_duration_seconds`, `anomaly_change_point_total`, `anomaly_confidence_score`, `anomaly_baseline_stability`, `anomaly_severity`, `anomaly_type_score`, and the composite `anomaly_composite_*` metrics — anywhere source labels could otherwise reach the wire.

**GDPR worked example.** A PromQL query like `rate(api_requests_total{customer_id=~".+"}[5m])` carries `customer_id` on each emitted series. Without filtering, every `anomaly_score` line includes the customer id; for a regulated workload that is itself a compliance event. Add:

```yaml
exporter:
  output_labels:
    drop:
      - customer_id
```

and the output collapses to `anomaly_score{id="api_request_rate", group="...", instance="...", region="..."}` — Prometheus and Grafana still see useful labels, but the customer id never leaves the detector pod.

## Fleet Anomaly-Density Rollup

"Is anything weird right now?" is the on-call's first question, and you don't want to scan hundreds of per-series gauges to answer it. After every group run the runner emits a **bounded-cardinality** rollup computed from that run's own samples — no extra TSDB queries, **no new per-series cardinality**:

| Metric | Labels | Meaning |
|---|---|---|
| `anomaly_active_series` | `group` | count of series currently outside threshold (any detector firing) |
| `anomaly_density` | `group` | active ÷ total, `0–1` |
| `anomaly_density` | `group, detector` | per-detector firing fraction |
| `anomaly_density` | `group, by, <label>` | per-slice fraction (one per `density_by` label value) |
| `anomaly_severity_density` | `group` | Σ per-series max severity ÷ total — ranks slices by *how bad*, not just *how many* |
| `anomaly_severity_density` | `group, by, <label>` | severity-weighted per-slice |

A *series* here is one `(id, group, source-labels)` identity — the per-detector `detector` / `detector_instance` labels are stripped so a series counts once regardless of how many detectors scored it. Warming-up series and fully-suppressed groups contribute nothing (no settled verdict), so the rollup appears as soon as any series produces a verdict.

The group-level rollup carries **only** `group`; the partitions add `detector` or `by`. To select just the group rollup in PromQL and avoid double-counting the partitions, match the absent partition labels:

```promql
# Group rollup only (exclude detector / by partitions).
anomaly_density{detector="", by=""}

# Which namespace is the unhealthiest right now?
topk(1, anomaly_density{by="namespace"})
```

`FleetAnomalyDensityHigh` (`anomaly_density{detector="", by=""} > 0.1 for 5m`) ships in `examples/alerts/promanomaly-rules.yaml` and the bundled Helm `PrometheusRule`. The `dashboards/grafana/promanomaly-fleet-density.json` dashboard is the single pane that makes promanomaly a triage source of truth.

### Choosing partition dimensions

```yaml
defaults:
  density_by:
    - namespace   # one anomaly_density{by="namespace", namespace=<v>} series per distinct value
```

Each listed label adds one series per distinct value seen in the run, so partition by **low-cardinality** dimensions (namespace, job, region) — never by `instance` or `pod` on a large fleet. Partitioning by a canonical detector label (`id`, `group`, `detector`, `detector_instance`) is rejected at config-load time. As a backstop against a high-cardinality mistake, a dimension that produces more than 200 distinct slice values in a run is **skipped for that run** and logged as `density_by_cardinality_capped` (the group rollup and per-detector partition are unaffected) — point the dimension at a lower-cardinality label and the slices return. Cohort divergence rolls up for free: `anomaly_density{group, detector="Cohort"}` is "members currently diverging ÷ cohort members scored".
## Self-Observability

The detector emits telemetry about its **own** health so a silently degrading component is caught before it stops catching anomalies. All four families are bounded by the configured group / detector cardinality — none touch the per-series budget.

| Metric | Labels | Meaning |
|--------|--------|---------|
| `anomaly_detect_success_ratio` | `group, detector, window` | Fraction of attempted detector computations that completed without raising (timeout / exception), over a rolling window. Two windows are emitted: `1h` (spots a fresh regression) and `1d` (the slower trend). |
| `anomaly_group_cpu_seconds_total` | `group` | Cumulative detector compute time attributed to a group (sum of per-detector wall-clock). Use `rate()` to see which group dominates the compute budget. |
| `anomaly_group_memory_bytes` | `group` | Best-effort size of the per-group output snapshot held by the exporter — answers "which group's snapshot is dominant", not an exact RSS breakdown. |
| `anomaly_snapshot_age_seconds` | `group` | Age of the group's most recent successful snapshot. Recomputed on every scrape, so it keeps climbing between runs and resets on each successful run. |
| `anomaly_series_staleness_seconds` | `group` | Age of the *freshest per-series score* in a group. Diverges from snapshot age when a group keeps writing snapshots that carry no scores (all series warming up, or scores dropped under `drop_scores`); absent until the group has produced at least one score. |

The success ratio is recorded once per group-run per detector (aggregating the run's per-series attempts), so the rolling ledger stays bounded at runs-per-window regardless of fleet size. CPU is the per-run sum of detector durations; memory is sampled at the end of each successful run. Snapshot-age and series-staleness are derived at scrape time from the snapshot-store timestamps.

Two reference alerts ship for this surface in `examples/alerts/promanomaly-rules.yaml` (and the bundled Helm `PrometheusRule`):

- **`AnomalyDetectorDegraded`** — `anomaly_detect_success_ratio{window="1h"} < 0.9 for 10m`. A detector is failing or timing out on too many series; its scores are silently missing before any per-series anomaly alert can fire.
- **`AnomalySnapshotStale`** — `anomaly_snapshot_age_seconds > 600 for 5m`. The served `/metrics` snapshot has aged out. Distinct from `AnomalyStale`, which keys off the last *successful-run* timestamp — under `serve_stale` the run timestamp can stop advancing while `/metrics` keeps returning an ever-older snapshot.

The `dashboards/grafana/promanomaly-self-observability.json` dashboard graphs all five families. See the [degraded-mode playbook](operations/degraded-modes.md) for the full failure-mode matrix.

## Warm-Up Status Endpoint

On a fresh install or after a restart, `/metrics` is empty until each series fills its rolling window (`min_points`). The opt-in `GET /warmup` endpoint (enable with `server.expose_warmup_endpoint: true`) makes that self-explaining: per group and query it reports points available versus needed, an ETA at the current `refresh_interval`, and the blocker (`min_points`, `lookback`, a stratified `baseline` still filling its multi-week lookback, or `discovery`).

```bash
promanomaly warmup --target http://localhost:9092
```

Each call issues one cheap probe per query — off by default because that cost is wasted once past the first install. The endpoint is in-namespace only (the bundled NetworkPolicy denies external ingress alongside `/debug/*`). The `promanomaly warmup` CLI exits non-zero while anything is still warming, so `until promanomaly warmup --target ...` is a valid post-install gate. See [`cli.md`](cli.md) for the command reference.

## End-to-End Detection Self-Test (dead-man's-switch)

The self-observability success ratios prove each detector *runs*; they do not prove the pipeline actually *catches* anomalies. A global misconfiguration — an absurd `alert_thresholds.score`, an exporter regression, a threshold mistake — can leave the process up and emitting nothing while every success ratio still reads 1.0. Enable the self-test to guard against exactly that:

```yaml
server:
  selftest:
    enabled: true       # off by default
    detector: MAD       # detector run through the synthetic path
    threshold: 3.0      # threshold the synthetic score is checked against
```

Each scheduled run drives one synthetic series carrying a known injected point-spike anomaly through the **real** detect → threshold → export path (the sample is even re-rendered through the exporter, so an exporter regression that rejects it also trips the test) and asserts it is caught. It publishes:

| Metric | Labels | Meaning |
|--------|--------|---------|
| `anomaly_selftest_ok` | `detector` | 1 when the injected anomaly was caught on the last run, 0 when the detect/threshold/export path failed to surface it. |
| `anomaly_selftest_failures_total` | `detector` | Cumulative failed self-test runs. |

Both metrics carry a `detector` label, so the series is **absent** (and the `AnomalyPipelineDead` alert dormant) on a deployment that hasn't opted in — a disabled self-test never looks like a dead pipeline. Stateless and bounded to one synthetic series; leader-gated in HA mode so the gauge is published once per cluster. The `AnomalyPipelineDead` reference alert (`anomaly_selftest_ok == 0 for 5m`, critical) ships in `examples/alerts/promanomaly-rules.yaml` and the Helm `PrometheusRule`. Distinct from the self-monitoring example config, which watches the detector's *operational* metrics rather than re-driving the detection path. See the [degraded-mode playbook](operations/degraded-modes.md).

## Config Health and Tuning Diagnostics

`promanomaly diagnose` answers "which of my detectors are mis-tuned?" from evidence. Against the TSDB scraping the detector's own metrics it flags, over a lookback: detectors that **never fire** (dead config), detectors that **fire too often** (threshold too low / wrong detector), **chronically-warming** series, **empty queries**, and — with `--config` — **silent detectors** (a configured detector emitting nothing, which includes a cohort below `min_cohort_size`). A lighter `--target` mode reads one `/metrics` scrape for a current-state view.

```bash
promanomaly diagnose --datasource-url http://victoriametrics:8428 --window 24h
promanomaly diagnose --target http://localhost:9092
```

Pure analysis, no persisted state; pairs with `backtest` and `calibrate-buckets` to close the tuning loop. See [`cli.md`](cli.md) for the full flag reference.

## Cloud-Managed Datasource Authentication

When metrics live in a managed Prometheus-compatible backend (Amazon Managed Service for Prometheus, Google Managed Prometheus, Azure Monitor), `datasource.auth.type` supports four cloud-native auth modes alongside the existing `bearer` / `basic` / `mtls`:

| Type | Backend | Credential source |
|------|---------|-------------------|
| `sigv4` | Amazon Managed Service for Prometheus | Standard AWS credential chain (instance/pod role, env, shared config) |
| `gcp` | Google Managed Prometheus / Cloud Monitoring | Application Default Credentials / Workload Identity |
| `azure` | Azure Monitor managed Prometheus | `DefaultAzureCredential` (Managed Identity, Workload Identity, az-cli) |
| `oauth2` | Any backend with a token endpoint | Client-credentials grant against a configurable `token_url` |

Each cloud SDK ships as an optional extra so the base image stays slim:

```bash
pip install promanomaly[aws]    # sigv4
pip install promanomaly[gcp]    # gcp
pip install promanomaly[azure]  # azure
```

A missing SDK surfaces an actionable `ImportError` at boot rather than a cryptic failure from the source layer. Token refresh is transparent to the retry/timeout wrapper.

### Worked example: Amazon Managed Service for Prometheus

```yaml
datasource:
  url: https://aps-workspaces.us-east-1.amazonaws.com/workspaces/ws-xxxxx/api/v1/
  auth:
    type: sigv4
    sigv4:
      region: us-east-1
```

### Worked example: Google Managed Prometheus

```yaml
datasource:
  url: https://monitoring.googleapis.com/v1/projects/my-project/location/global/prometheus/
  auth:
    type: gcp
    gcp: {}   # uses Application Default Credentials
```

### Worked example: Azure Monitor

```yaml
datasource:
  url: https://my-monitor.eastus.prometheus.monitor.azure.com/
  auth:
    type: azure
    azure:
      client_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
      tenant_id: "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
      client_secret_file: /etc/promanomaly-datasource-auth/client_secret
```

### Worked example: Generic OAuth2

```yaml
datasource:
  url: https://metrics.example.com/
  auth:
    type: oauth2
    oauth2:
      token_url: https://auth.example.com/oauth2/token
      client_id: promanomaly
      client_secret_file: /etc/promanomaly-datasource-auth/client_secret
      scopes:
        - metrics:read
```

## Multi-Tenant Datasource Support

For multi-tenant Mimir or Cortex backends, a `tenant` field injects the `X-Scope-OrgID` header (or a custom header) on every PromQL request:

```yaml
datasource:
  url: http://mimir-query-frontend:8080/
  tenant:
    id: team-platform
    header: X-Scope-OrgID   # default; configurable for non-standard backends
```

Per-group overrides let a single detector deployment query multiple tenants:

```yaml
groups:
  - name: team_a_errors
    tenant:
      id: team-a
    queries:
      - id: error_rate
        promql: 'rate(http_requests_total{code=~"5.."}[5m])'
        detectors: [MAD]
  - name: team_b_errors
    tenant:
      id: team-b
    queries:
      - id: error_rate
        promql: 'rate(http_requests_total{code=~"5.."}[5m])'
        detectors: [MAD]
```

The tenant value is **not** emitted as an output label by default (it is a query-side concern, not a signal-side one), keeping the cross-tool `(id, group)` join contract intact. The query-cache key includes the tenant so two tenants' identical PromQL do not collide on a cached result.

## Config Composition for Multi-Team Ownership

When many teams contribute detector config under GitOps, splitting the monolithic YAML into per-team fragments avoids merge conflicts. `--config` accepts a directory (or a glob) in addition to a single file:

```bash
promanomaly --config /etc/promanomaly/config.d/
promanomaly validate --config /etc/promanomaly/config.d/
```

**Directory layout:**

```
config.d/
  _defaults.yaml       # datasource, server, safety, defaults, exporter
  team_platform.yaml   # groups owned by the platform team
  team_payments.yaml   # groups owned by the payments team
```

`_defaults.yaml` carries the base settings (`datasource`, `server`, `safety`, `defaults`, `exporter`). Every other `*.yaml` fragment's `groups:` are merged in deterministic filename-sorted order. Duplicate group names across fragments are a hard validation error (not a silent last-wins).

`validate`, `validate --probe`, and `--estimate-cost` all accept the directory form so CI gates work unchanged. `anomaly_config_hash` reflects the merged document.

The Helm chart supports this via `existingConfigMap` pointing at a ConfigMap you manage out-of-band (Argo CD app-of-apps, Flux Kustomization).

## Config Schema Migration

`promanomaly migrate-config` upgrades config files across schema/apiVersion changes:

```bash
# Preview changes without writing
promanomaly migrate-config --config config.yaml --dry-run

# Apply migration in place
promanomaly migrate-config --config config.yaml

# Migrate all files in a directory
promanomaly migrate-config --config config.d/
```

Idempotent: running on an already-current config is a no-op. Migrations are registered per schema revision so the path composes across multiple versions.

## Metadata-Aware Validation Lint

`promanomaly validate --lint-metadata` queries the datasource's `/api/v1/metadata` and warns when a query feeds a raw counter to a detector without a `rate()` / `increase()` wrapper — a monotonic counter scored directly produces nonsense. Lint only (it never rewrites the query); under `--strict` any finding fails CI, and it composes with `--probe` / `--estimate-cost`. The same hint surfaces live in `inspect` and `promanomaly top --lint`. Absent metadata is treated as "no opinion", so the lint never manufactures a false positive from a metric the TSDB carries no type for. See [`cli.md`](cli.md).
