# Sinks

By default promanomaly is **pull-only**: Prometheus scrapes `/metrics`.  

You can optionally add **push sinks** that forward each successful group snapshot to external systems after every run. Sinks are *additional* — they never replace the `/metrics` endpoint.

## Failure Isolation

A sink failure is completely isolated:
- It is logged and counted in `anomaly_sink_failures_total{sink, reason}` (bounded cardinality).
- It **never** blocks `/metrics`, readiness, or the detection pipeline.
- Common reasons: `http_error`, `timeout`, `connect_error`, `serialize_error`, `rate_limited`, `exception`.

In HA mode only the leader pushes, so each snapshot is sent exactly once (brief duplicates during failover are harmless).

## Available Sinks

### `remote_write` — Long-horizon anomaly history

Pushes snapshots to a Prometheus remote-write endpoint. Ideal for keeping `anomaly_density` and other signals beyond scrape retention.

```yaml
sink:
  type: remote_write
  remote_write:
    url: http://victoriametrics:8428/api/v1/write
    timeout: 10s
    auth:
      type: bearer          # none | bearer | basic | mtls
      token_file: /etc/promanomaly-sink-auth/remote-write/token
```

### `grafana_annotations` — Anomalies on every dashboard

Posts an annotation to Grafana on every `0→1` transition of `anomaly_outside_threshold` and every `anomaly_change_point_total` increment. Annotations appear across **all** dashboards that match the tags.

```yaml
sink:
  type: grafana_annotations
  grafana_annotations:
    url: http://grafana.monitoring.svc:3000
    timeout: 10s
    auth:
      type: bearer
      token_file: /etc/promanomaly-sink-auth/grafana-annotations/token
    tags:
      - promanomaly
    max_annotations_per_run: 50
```

**Key behaviours**
- Stateless across restarts (only new 0→1 transitions are annotated).
- Rate-limited per run to protect the Grafana API.
- Add a dashboard annotation query filtering on your tags to see vertical markers.

## Multiple Sinks

To push to several destinations at once, use the `sinks:` list:

```yaml
sinks:
  - type: remote_write
    remote_write:
      url: http://victoriametrics:8428/api/v1/write
      auth:
        type: bearer
        token_file: /etc/promanomaly-sink-auth/remote-write/token
  - type: grafana_annotations
    grafana_annotations:
      url: http://grafana.monitoring.svc:3000
      auth:
        type: bearer
        token_file: /etc/promanomaly-sink-auth/grafana-annotations/token
```

- `sink:` and `sinks:` are mutually exclusive.
- Each sink type may appear at most once.
- Failures are isolated per sink.

## Helm Configuration

Both single and multiple sinks are fully configurable in the detector chart via the `sink:` (scalar) or `sinks:` (list) block.  
Credentials are mounted from Secrets — never inline tokens. See `charts/promanomaly/values.yaml`.

## Why No Built-in Alerting Engine?

Sinks only forward **signal**. Alerting decisions, flap suppression, deploy silencing, and routing belong in Alertmanager and the rest of the Prometheus stack — not the detector.

This keeps promanomaly focused, stateless, and easy to operate.