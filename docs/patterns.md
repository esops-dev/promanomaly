# Composition Patterns Cookbook

Most advanced use cases are solved by combining promanomaly’s plain Prometheus metrics (`anomaly_score`, `anomaly_outside_threshold`, etc.) with standard PromQL, recording rules, or Alertmanager. No new detector features required.

## Rate-of-Change & Acceleration

Detect anomalies in *how fast* a metric changes, not its absolute value.

```yaml
queries:
  - id: queue_growth
    promql: rate(queue_depth_total[5m])
    detectors:
      - name: MAD

  - id: queue_acceleration
    promql: deriv(rate(queue_depth_total[5m])[15m:1m])
    detectors:
      - name: Hampel
```

Detectors work on any numeric series — rates and derivatives are handled identically to raw values.

## Group-Level Rollups

One alert per group instead of per series.

```yaml
# recording rules
- record: anomaly_outside_threshold:group_any
  expr: max by (group) (anomaly_outside_threshold)

- record: anomaly_outside_threshold:group_count
  expr: count by (group) (anomaly_outside_threshold == 1)

- record: anomaly_score:group_max
  expr: max by (group) (anomaly_score)
```

Example alert:
```yaml
- alert: AnomalyGroupBurst
  expr: anomaly_outside_threshold:group_count > 10
  for: 5m
```

## Flap Suppression

Use the standard alert `for:` clause:

```yaml
- alert: AnomalyOutsideThreshold
  expr: anomaly_outside_threshold == 1
  for: 5m
```

For duration-based alerts use the built-in `anomaly_duration_seconds` metric.

## Multi-Window Detection

Catch both short spikes and longer trends with the same detector:

```yaml
detectors:
  - name: MAD
    instance: short
    params:
      window: 5m
  - name: MAD
    instance: long
    params:
      window: 1h
```

The `detector_instance` label separates the outputs.

## Absence Detection

For metrics that *should* exist:

```yaml
queries:
  - id: heartbeat
    promql: up{job="critical"}
    expect: true
    expect_grace_runs: 3
    detectors:
      - name: MAD
```

This emits `anomaly_signal_missing` when the series disappears.

For unwatched metrics use `absent_over_time()` directly in alerts.

## Per-Group Thresholds

Override the default only where needed:

```yaml
defaults:
  alert_thresholds:
    score: 3.0

groups:
  - name: critical
    queries:
      - id: checkout
        promql: ...
        alert_thresholds:
          score: 2.5
        detectors:
          - name: MAD
```

## Top-N Anomalies Panel (Grafana)

```promql
topk(10, max by (id, group, instance) (anomaly_score))
```

Add `anomaly_duration_seconds` as a second column to distinguish brief spikes from sustained problems.

## Change-Points Composed With Outlier Alerts

Layer change-point detection on top of an ordinary outlier detector so the alert text can distinguish "still anomalous" from "regime just shifted":

```yaml
queries:
  - id: error_rate
    promql: sum by (service) (rate(http_requests_total{status=~"5.."}[5m]))
    detectors:
      - name: MAD                 # ordinary outlier alerts
      - name: BOCPD               # regime-shift detection
      - name: CUSUM               # classical change-point counter
```

Alert recipes:

```promql
# Regime shift happened recently AND the latest sample is still off-baseline.
increase(anomaly_change_point_total{detector="BOCPD"}[10m]) > 0
  and on (id, group) anomaly_outside_threshold{detector="BOCPD"} == 1
```

```promql
# Either change-point detector says "fresh fire in the last minute".
sum by (id, group) (
  rate(anomaly_change_point_total{detector=~"BOCPD|CUSUM"}[1m])
) > 0
```

Do NOT silence change-point alerts during deploys inside the detector — fire on the change-point in Prometheus, silence in Alertmanager during the deploy window via webhooks. Damping scores during deploys hides the failure modes operators most need to see.

## Cross-Tool: promforecast + promanomaly

```promql
(forecast_deviation_outside_band == 1)
  and on (id, group)
(anomaly_outside_threshold == 1)
```

Both tools follow the same `id` + `group` label contract.
