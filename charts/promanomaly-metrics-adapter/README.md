# promanomaly-metrics-adapter

An **opt-in** Kubernetes external/custom metrics adapter for promanomaly.

It is a stateless aggregated apiserver that re-serves the anomaly metrics
the detector already wrote to the TSDB through
`external.metrics.k8s.io` / `custom.metrics.k8s.io`, so your existing HPA
and KEDA tooling can consume anomaly signal like any other metric.

promanomaly stays the metrics **provider**; the autoscaler stays the
**decision-maker**. The adapter never makes a reaction decision itself.

## Guard-rails (read first)

Anomaly signal is a *sharp* scaling input. Wire it carefully:

- **Scale on `anomaly_density`**, not raw `anomaly_score`. "Add workers
  when the backlog is anomalously deep" is sound; "scale on how
  anomalous a latency metric looks" *amplifies* incidents — a latency
  spike scales you up, which can deepen the spike.
- **Gate on `anomaly_confidence_score` and `anomaly_duration_seconds`.**
  React to sustained, high-confidence anomalies, not single-tick blips.
- **Always pair an anomaly-driven scaler with a reactive fallback HPA**
  (CPU/memory or queue length). If the detector or TSDB is unavailable,
  the fallback keeps the workload safe.

The default `metrics` list deliberately omits scaling presets for
`anomaly_score`. Worked recipes with these guard-rails baked in live in
[`examples/k8s/`](../../examples/k8s/).

## What it exposes

| Metric | Good for |
| --- | --- |
| `anomaly_density` | Fraction of a group currently anomalous — the recommended scaling input. |
| `anomaly_severity` | Operator-facing 0-1 severity; gate scaling on it. |
| `anomaly_active_series` | Count of firing series in a group. |
| `anomaly_outside_threshold` | Per-series firing flag (0/1). |

Both API groups are served:

- **`external.metrics.k8s.io`** — cluster-scoped, the path KEDA's
  `external` trigger and HPA `External` metric source use. Recommended.
- **`custom.metrics.k8s.io`** — the same signals associated with objects
  (pods, namespaces) for HPA `Object`/`Pods` rules.

## Install

```bash
helm install promanomaly-adapter charts/promanomaly-metrics-adapter \
  --namespace monitoring \
  --set datasource.url=http://victoriametrics.monitoring.svc:8428/
```

Verify:

```bash
kubectl get apiservices | grep metrics.k8s.io
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1" | jq .
```

## TLS

The aggregation layer only talks HTTPS to an APIService backend, so a
serving cert is mandatory.

- **Default**: the chart mints a self-signed CA + serving cert and wires
  the APIService `caBundle` to it. Simple, but it **regenerates on every
  `helm upgrade`** (a brief reconcile blip).
- **Rotation-free**: set `tls.existingSecret` to a `kubernetes.io/tls`
  Secret (e.g. from cert-manager) and `tls.caBundle` to its CA. No churn.
- **Skip verification**: `apiService.insecureSkipTLSVerify: true` if you
  accept the aggregation layer not verifying the adapter cert.

## Key values

| Key | Default | Description |
| --- | --- | --- |
| `datasource.url` | `http://victoriametrics:8428/` | TSDB the detector writes to. |
| `datasource.auth.type` | `none` | `none`/`bearer`/`basic`/`mtls`. |
| `datasource.auth.existingSecret` | `""` | Secret with the credential (keys: `token`/`password`/`ca.crt`+`tls.crt`+`tls.key`), mounted and referenced by file. |
| `metrics` | the four above | Anomaly metrics to expose. |
| `customResources` | `[pods, namespaces]` | Object kinds for custom metrics. |
| `tls.existingSecret` | `""` | Use a managed serving cert instead of generating one. |
| `apiService.enabled` | `true` | Register the two APIService objects. |
| `apiService.insecureSkipTLSVerify` | `false` | Skip aggregation-layer cert verification. |
| `rbac.create` | `true` | auth-delegator + auth-reader + HPA metrics-reader RBAC. |
| `networkPolicy.enabled` | `true` | Restrict ingress to the serving port. |

## How it differs from k8s-prometheus-adapter

This adapter is purpose-built for the bounded set of anomaly metrics
promanomaly emits, with the guard-rails documented above. It reads the
same TSDB the detector writes to and translates label selectors directly
into PromQL matchers. For arbitrary Prometheus metrics, use the
general-purpose [prometheus-adapter](https://github.com/kubernetes-sigs/prometheus-adapter).
