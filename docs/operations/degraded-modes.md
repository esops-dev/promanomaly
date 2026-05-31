# Degraded-Mode Operations Playbook

promanomaly isolates failures at every layer and keeps serving the last good snapshot during transient TSDB issues. This resilience is intentional — but it also means several degraded states are **invisible** if you only monitor whether the pod is Running.

This playbook lists every failure mode, the exact signal that detects it, and the recovery steps. The shipped alerts in [`examples/alerts/promanomaly-rules.yaml`](../../examples/alerts/promanomaly-rules.yaml) cover all of them.

## Failure-Isolation Layers

A single fault never takes down more than its own layer:

| Layer          | Caught by                          | Continues running?                  |
|----------------|------------------------------------|-------------------------------------|
| Per-detector   | `try/except` in scoring loop       | Yes — other detectors on the series emit |
| Per-series     | `try/except` inside a query        | Yes — sibling series continue       |
| Per-query      | `try/except` per query             | Yes — other queries in the group continue |
| Per-group      | `try/except` at group level        | Yes — sibling groups continue       |
| TSDB-level     | `safety.on_source_failure` policy  | Yes — last good snapshot served     |

All failures increment `anomaly_failures_total{group, detector, reason}` (bounded cardinality).

## `safety.on_source_failure` Policy

| Policy               | Snapshot on failure                          | Readiness                  | Use when |
|----------------------|----------------------------------------------|----------------------------|----------|
| `serve_stale` (default) | Last good snapshot kept unchanged           | Unaffected                 | You prefer stale data over blank charts |
| `drop_scores`        | Failing series dropped; others stay fresh (full-group failure degrades to `serve_stale`) | Unaffected | You prefer staleness handling over stale values |
| `fail_ready`         | Same as `serve_stale`                        | `anomaly_group_ready` → 0 after N failures | You want the pod to shed traffic on sustained TSDB outage |

`anomaly_source_failure_streak{group}` is always published so you can alert *before* `fail_ready` trips.

## Failure-Mode Matrix

| Failure mode                  | Detection signal                                      | Recovery |
|-------------------------------|-------------------------------------------------------|----------|
| TSDB unreachable              | `rate(anomaly_source_failure_total[15m]) > 0`        | Check TSDB reachability / auth / network |
| Partial group failure         | `anomaly_failures_total{group, reason} > 0`          | Fix PromQL or cardinality, then reload |
| Sustained query timeout       | `rate(anomaly_failures_total{reason="timeout"}[15m])` | Raise timeouts or lighten config |
| Stale snapshot                | `anomaly_snapshot_age_seconds > 600` or `time() - anomaly_last_run_timestamp_seconds > 600` | Check pod health + rows above |
| Detector degrading            | `anomaly_detect_success_ratio{window="1h"} < 0.9`   | Inspect failures by detector |
| Dead detection pipeline       | `anomaly_selftest_ok == 0` (opt-in self-test)        | Check threshold / exporter regression / global misconfig |
| Leader churn (HA)             | `rate(anomaly_leader_transitions_total[15m]) > 2`    | Check Lease contention / pod restarts |
| Group starvation (shedding)   | `rate(anomaly_group_skipped_total[15m]) > 0`         | Raise max_concurrent_groups / lengthen refresh / scale out |

## TSDB Read Failure

**Detection**  
`rate(anomaly_source_failure_total[15m]) > 0`

**Recovery**  
Check VictoriaMetrics/Mimir/Thanos health, network policies, and auth. Under `serve_stale` the `/metrics` endpoint still serves the last good snapshot.

**Shipped alert:** `AnomalySourceFailing`

## Partial Group Failure

**Detection**  
Non-zero `anomaly_failures_total{group, reason}` for one group while others are healthy.

**Recovery**  
Check JSON logs for the group + reason, fix the PromQL or cardinality issue, then `SIGHUP` or `POST /-/reload`.

## Sustained Query Timeout

**Detection**  
Rising `rate(anomaly_failures_total{reason="timeout"}[15m])` or `anomaly_detect_duration_seconds` approaching the timeout.

**Recovery**  
Increase `safety.query_timeout` / `safety.detect_timeout`, switch to a lighter detector, or split heavy queries into their own group.

## Stale Snapshot

**Detection**  
```promql
anomaly_snapshot_age_seconds > 600
or
time() - anomaly_last_run_timestamp_seconds > 600
```

**Recovery**  
Check pod health (`OOMKilled`, `Recreate` deploy gap) and the failure modes above.

**Shipped alerts:** `AnomalyStale`, `AnomalySnapshotStale`

## Detector Degrading

**Detection**  
`anomaly_detect_success_ratio{window="1h"} < 0.9`

**Recovery**  
Inspect `anomaly_failures_total` by detector and `anomaly_detect_duration_seconds`. Common causes: new detector version, parameter change, or pathological signal shape.

**Shipped alert:** `AnomalyDetectorDegraded`

## Dead Detection Pipeline (opt-in self-test)

Per-detector success ratios prove each detector *runs* without raising — they do **not** prove the pipeline actually catches anomalies. A global misconfiguration (an absurd `alert_thresholds.score`, an exporter regression, a threshold mistake) can leave the process up and emitting nothing while every success ratio still reads 1.0. The end-to-end self-test is the dead-man's-switch for exactly that.

**Enable**  
Set `server.selftest.enabled: true`. Each run drives a synthetic series with a known injected anomaly through the real detect → threshold → export path and asserts it is caught, publishing `anomaly_selftest_ok` (1/0) and `anomaly_selftest_failures_total`.

**Detection**  
`anomaly_selftest_ok == 0`

**Recovery**  
The detect/threshold/export path is failing to surface a known anomaly: check for a recently-changed global threshold, a detector parameter mistake, or an exporter regression. `anomaly_selftest_failures_total` confirms it is failing repeatedly rather than flapping.

**Shipped alert:** `AnomalyPipelineDead` (dormant unless the self-test is enabled — the series only exists when `server.selftest.enabled`).

## Leader Churn (HA mode only)

**Detection**  
`rate(anomaly_leader_transitions_total[15m]) > 2`

**Recovery**  
Check Lease contention, pod restarts, and `lease_duration` / `retry_period` tuning. Brief churn is safe; sustained churn wastes runs.

## Group Starvation (load shedding)

When `safety.max_concurrent_groups` is set, the scheduler is a priority-aware gate: under sustained TSDB backpressure it admits at most that many group runs at once, prefers higher-`priority` groups, and **sheds** (skips) a run rather than queuing it unboundedly. Like every other degraded mode here, shedding is **visible, never silent** — a skip delays a score, it never suppresses a detected anomaly (the last good snapshot stays in place per `serve_stale`).

| Reason | Meaning |
|--------|---------|
| `backpressure` | No concurrency slot freed within the group's refresh interval under `max_concurrent_groups`. |
| `overrun` | The group's previous run was still in flight when the next tick fired (it can't keep up with its own cadence). |

**Detection**  
`rate(anomaly_group_skipped_total[15m]) > 0`

**Recovery**  
Sustained shedding means the detector is starved of capacity for the load. Options, cheapest first:

- Raise `safety.max_concurrent_groups` (if CPU/TSDB headroom exists).
- Lengthen the starved group's `refresh_interval` (per-group override) so it demands fewer passes.
- Split the group's heavy queries, or move expensive stratified/cohort groups to their own deployment.
- Scale out (`scaleOut.enabled`) so multiple replicas share the work.

A `backpressure` skip on a **low**-priority group while high-priority groups stay fresh is the gate working as designed — raise the starved group's `priority` only if it genuinely matters more.

**Shipped alert:** `AnomalyGroupStarved`

## Putting It Together

- **Page** on `AnomalySourceFailing`, `AnomalyStale`, `AnomalySnapshotStale`, and (if the self-test is enabled) `AnomalyPipelineDead`.
- **Ticket** on `AnomalyDetectorDegraded` and `AnomalyGroupStarved`.

All other silencing, deploy damping, and flap suppression belongs in Alertmanager (silences + `for:` clauses).

## Cross-References

- [Production-readiness checklist](../production-checklist.md)
- [Operations guide](../operations.md)
- [Configuration schema](../config-schema.md)