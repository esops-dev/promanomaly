# promanomaly Helm Chart

This chart installs the promanomaly detector as a single `Deployment`, with a `Service` for `/metrics`, a `ConfigMap` for your configuration, an optional `ServiceMonitor`, an optional `PrometheusRule` with reference alerts, and a `NetworkPolicy` that locks down `/-/reload` and debug endpoints by default.

Bring your own Prometheus or VictoriaMetrics. For a full stack (including VictoriaMetrics), use the `promanomaly-stack` umbrella chart.

## Quick Start

```bash
helm repo add promanomaly https://esops-dev.github.io/promanomaly

helm install my-detector promanomaly/promanomaly \
  --set datasource.url=http://victoriametrics:8428/ \
  --values my-groups.yaml
```

`my-groups.yaml` contains your detector groups (under the `groups:` key).

## Key Configuration Options

| Value                              | Default     | Description |
|------------------------------------|-------------|-------------|
| `replicaCount`                     | `1`         | Single replica is simplest and fully supported |
| `highAvailability.enabled`         | `false`     | Enables leader election + Redis-backed snapshot cache for HA |
| `existingConfigMap`                | —           | Use your own ConfigMap (GitOps-friendly) |
| `datasource.auth`                  | `none`      | Supports `none` / `bearer` / `basic` / `mtls` |
| `datasource.auth.existingSecret`   | —           | Secret for credentials (mounted as files) |
| `networkPolicy.enabled`            | `true`      | Locks down sensitive endpoints |
| `serviceMonitor.enabled`           | `false`     | Creates ServiceMonitor for Prometheus Operator |
| `serviceMonitor.honorLabels`       | `true`      | Required for correct label propagation |
| `prometheusRule.enabled`           | `false`     | Installs reference alerts (tunable thresholds) |

Full reference in `values.yaml` + `values.schema.json`.

## Local Linting

```bash
helm dependency update charts/promanomaly

helm lint --strict charts/promanomaly

helm template charts/promanomaly | kubeconform -strict -summary -ignore-missing-schemas
```

That’s it. The chart is deliberately minimal and production-ready out of the box. See the [production-readiness checklist](../production-checklist.md) for the full go-live steps.