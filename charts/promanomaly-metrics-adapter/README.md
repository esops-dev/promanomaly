# promanomaly Metrics Adapter

An **opt-in**, stateless Kubernetes external/custom metrics adapter for promanomaly.

It re-exposes the anomaly metrics written by the detector to the TSDB through the standard Kubernetes `external.metrics.k8s.io` and `custom.metrics.k8s.io` APIs. This lets HPA and KEDA scale on anomaly signals exactly like any other metric.

promanomaly is the **metrics provider**. The autoscaler remains the **decision maker**.

## Guard-rails (Read First)

Anomaly signals are sharp scaling inputs. Use them carefully:

- **Scale on `anomaly_density`** (fraction of a group that is anomalous), not raw `anomaly_score`.
- **Gate on `anomaly_severity`** (already includes confidence and duration).
- **Always pair** an anomaly-driven scaler with a traditional reactive HPA fallback (CPU, memory, or queue depth).

Safe configuration examples are in [`examples/k8s/`](../../examples/k8s/).

## Exposed Metrics

| Metric                        | Recommended Use                              |
|-------------------------------|----------------------------------------------|
| `anomaly_density`             | Primary scaling signal (fraction anomalous)  |
| `anomaly_severity`            | 0–1 normalized severity for gating           |
| `anomaly_active_series`       | Count of currently firing series             |
| `anomaly_outside_threshold`   | Per-series 0/1 firing flag                   |

Both API groups are supported:
- `external.metrics.k8s.io` — recommended for KEDA and HPA `External` metrics.
- `custom.metrics.k8s.io` — for HPA `Object` and `Pods` metrics.

## Quick Install

```bash
helm install promanomaly-adapter charts/promanomaly-metrics-adapter \
  --namespace monitoring \
  --set datasource.url=http://victoriametrics.monitoring.svc:8428/
```

Verify it is registered:

```bash
kubectl get apiservices | grep metrics.k8s.io

kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1" | jq .
```

## TLS

The Kubernetes aggregation layer requires HTTPS.

The chart can:
- Automatically generate a self-signed serving certificate (default, simple).
- Use an existing `kubernetes.io/tls` Secret via `tls.existingSecret` (recommended for production, e.g. from cert-manager).

## Key Configuration Values

| Key                                 | Default                  | Description |
|-------------------------------------|--------------------------|-------------|
| `datasource.url`                    | —                        | URL of the TSDB written by the detector |
| `datasource.auth.type`              | `none`                   | `none` / `bearer` / `basic` / `mtls` |
| `datasource.auth.existingSecret`    | `""`                     | Secret containing credentials |
| `metrics`                           | Core anomaly metrics     | Metrics to expose |
| `customResources`                   | `[pods, namespaces]`     | Kubernetes objects for custom metrics |
| `tls.existingSecret`                | `""`                     | Use a managed TLS Secret |
| `apiService.insecureSkipTLSVerify`  | `false`                  | Skip TLS verification (not recommended in production) |
| `networkPolicy.enabled`             | `true`                   | Restrict ingress |

## How It Differs from prometheus-adapter

This adapter is purpose-built and opinionated specifically for promanomaly’s anomaly metrics. It includes built-in guard-rails and directly translates label selectors into PromQL.

For exposing arbitrary Prometheus metrics, use the general-purpose [prometheus-adapter](https://github.com/kubernetes-sigs/prometheus-adapter) instead.