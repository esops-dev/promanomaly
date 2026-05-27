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

Single-replica only. The Redis-backed HA variant ships in v0.4 and plugs into the same call site without changing dashboards.

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