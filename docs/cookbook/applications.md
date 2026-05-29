# Applications & Middleware Signals

Recipes for application-tier metrics that don’t come from node-exporter: queue lag, HTTP errors, latency, and missing heartbeats.

Complete starter configs are in  
[`examples/configs/applications.yaml`](../../examples/configs/applications.yaml).

---

## Kafka Consumer Lag

**Why it matters.** Growing lag means consumers can’t keep up — the classic precursor to a backlog incident. The signal often drifts upward rather than spiking.

**Recommended detectors.** `Hampel` (handles drifting baseline) + `CUSUM` (catches full stalls).

```yaml
apiVersion: promanomaly.io/v1
datasource:
  url: http://victoriametrics:8428/
defaults:
  window: 1h
  step: 30s
  min_points: 30
groups:
  - name: kafka_consumer_lag
    queries:
      - id: kafka_consumergroup_lag
        promql: |
          sum by (consumergroup, topic) (kafka_consumergroup_lag)
        detectors:
          - name: Hampel
            params:
              window: 30m
              t0: 5
          - name: CUSUM
```

**Dashboard shape.** Lag near zero, then a sustained climb; CUSUM marks the exact stall point.

**Cardinality.** One series per `(consumergroup, topic)`.

**Example alert.**
```yaml
- alert: KafkaConsumerLagAnomaly
  expr: anomaly_outside_threshold{id="kafka_consumergroup_lag"} == 1
  for: 10m
```

---

## HTTP 5xx Rate

**Why it matters.** Rising 5xx errors are the most direct “users are seeing failures” signal.

**Recommended detectors.** `MAD` on the error fraction (with a floor) + optional `Cohort` to catch one bad backend.

```yaml
apiVersion: promanomaly.io/v1
datasource:
  url: http://victoriametrics:8428/
defaults:
  window: 1h
groups:
  - name: http_errors
    queries:
      - id: http_5xx_fraction
        promql: |
          sum by (service) (rate(http_requests_total{code=~"5.."}[5m]))
          / clamp_min(sum by (service) (rate(http_requests_total[5m])), 1)
        min_relative_delta: 0.05
        detectors:
          - name: MAD
```

**Dashboard shape.** Error fraction with baseline; firing on a sustained rise above recent normal.

**Cardinality.** One series per `service`.

**Example alert.**
```yaml
- alert: HTTP5xxAnomaly
  expr: anomaly_severity{id="http_5xx_fraction"} >= 0.7
  for: 5m
```

---

## HTTP p99 Latency

**Why it matters.** Tail latency is what users actually feel. A p99 regression often precedes a full outage.

**Recommended detector.** `Hampel` — handles the naturally drifting baseline of percentile series.

```yaml
apiVersion: promanomaly.io/v1
datasource:
  url: http://victoriametrics:8428/
defaults:
  window: 1h
groups:
  - name: http_latency
    queries:
      - id: http_request_duration_p99
        promql: |
          histogram_quantile(
            0.99,
            sum by (service, le) (rate(http_request_duration_seconds_bucket[5m]))
          )
        detectors:
          - name: Hampel
            params:
              window: 30m
              t0: 5
```

**Dashboard shape.** p99 line with baseline; a regression appears as a step the baseline trails.

**Cardinality.** One series per `service`.

**Example alert.**
```yaml
- alert: HTTPp99LatencyAnomaly
  expr: anomaly_outside_threshold{id="http_request_duration_p99"} == 1
  for: 5m
```

---

## Certificate / Heartbeat Absence

**Why it matters.** For heartbeats, scheduled jobs, or certificate exporters, **absence of the signal** *is* the anomaly. No statistical detector can catch a series that stopped emitting.

**Recommended approach.** `expect: true` static absence detection. Emits `anomaly_signal_missing` after a short grace period.

```yaml
apiVersion: promanomaly.io/v1
datasource:
  url: http://victoriametrics:8428/
defaults:
  window: 1h
groups:
  - name: heartbeats
    queries:
      - id: nightly_backup_heartbeat
        promql: backup_last_success_timestamp_seconds{job="nightly-backup"}
        expect: true
        expect_grace_runs: 3
        detectors:
          - name: MAD
```

**Dashboard shape.** A single `anomaly_signal_missing` gauge that flips to `1` when the heartbeat disappears.

**Cardinality.** One series (the absence signal is always bounded).

**Example alert.**
```yaml
- alert: HeartbeatMissingAnomaly
  expr: anomaly_signal_missing{id="nightly_backup_heartbeat"} == 1
  for: 10m
```

(Uses the bundled `AnomalySignalMissing` rule.)