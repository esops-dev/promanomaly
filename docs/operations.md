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