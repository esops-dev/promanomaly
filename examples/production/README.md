# Production reference deployment

A worked example of running promanomaly in production: two replicas
behind a Lease-based leader election, a shared Redis snapshot and
query cache, NetworkPolicy ingress + egress, anti-affinity, the
bundled PrometheusRule, and four detector groups sized for ~10k
output series.

This directory is meant to be **copied and tuned**, not consumed
as-is. The values reflect a generic mid-size cluster; bring your own
Prometheus, your own VictoriaMetrics, your own Redis, and your own
PromQL queries.

## Files

| File | Purpose |
|---|---|
| [`architecture.txt`](architecture.txt) | ASCII data-flow diagram of the deployed topology. |
| [`values.yaml`](values.yaml) | Helm values for `charts/promanomaly`. Includes the four detector groups inline so `helm template --values values.yaml` renders end-to-end with no external state. |
| [`networkpolicy-egress.yaml`](networkpolicy-egress.yaml) | Sibling `NetworkPolicy` constraining egress to the TSDB, Redis, and the Kubernetes apiserver only. The chart-shipped `NetworkPolicy` covers ingress; this one covers egress. |
| [`argocd/application.yaml`](argocd/application.yaml) | Argo CD `Application` with two-source chart+values pinning. |
| [`flux/helmrepository.yaml`](flux/helmrepository.yaml) | Flux `HelmRepository` resolving the chart. |
| [`flux/helmrelease.yaml`](flux/helmrelease.yaml) | Flux `HelmRelease` referencing `existingConfigMap` so the detector groups are managed separately from chart upgrades. |
| [`flux/configmap.yaml`](flux/configmap.yaml) | The standalone detector config the `HelmRelease` references. |

## Detector groups

Four groups, each demonstrating a different anomaly shape:

- **`error_rates`** — voting ensemble of `MAD` + `ZScoreEWMA`, gated by
  a 5 % relative-significance floor so noise on tiny ratios doesn't
  page. Highest priority.
- **`request_counts`** — multi-window `MAD` (`short` 15m + `long` 1h)
  plus `Hampel` for spike isolation. Sized for bursty traffic.
- **`queue_depths`** — `IQR` for distribution-tail anomalies on sparse
  signals, plus `BOCPD` for regime shifts.
- **`node_cohort`** — fleet-relative `Cohort` detection on CPU and
  memory across node-exporter targets. Catches the "one bad node out
  of 100" case forecast-based detection misses.

Total output series ≈ 10k assuming roughly 100 series per query and
the configured cohort sizes. Bump `safety.max_series_per_query` and
`safety.max_total_series` proportionally if your fanout is wider.

## Picking a GitOps tool

The two GitOps samples — Argo CD under `argocd/` and Flux under
`flux/` — are equivalent end-states. Pick one; **don't mix them**. The
two patterns differ in one meaningful way:

- **Argo CD's** sample uses a multi-source `Application` so chart
  version and values file are pinned independently in the same
  manifest.
- **Flux's** sample splits the values further: the chart-deployment
  knobs live in the `HelmRelease`, and the detector groups live in a
  sibling `ConfigMap` referenced via `existingConfigMap`. This keeps
  chart upgrades and detector tuning on independent PR cadences,
  which scales better for teams where SREs own the chart and data
  scientists own the detector groups.

Both pin `promanomaly` chart `1.0.0`. Bump via PR.

## Tuning checklist

Before applying any of this to a real cluster, walk through:

1. **Replace placeholder service names** — `victoria-metrics-single-server`,
   `promanomaly-redis-master`, and the namespaces (`observability`,
   `argocd`, `flux-system`) all need to match your cluster.
2. **Validate the egress NetworkPolicy CIDR** — the sample assumes
   the apiserver lives somewhere in `10.0.0.0/8`. Confirm with
   `kubectl get svc kubernetes -o yaml`.
3. **Run `promanomaly validate --probe --strict`** against your TSDB
   with these queries before merging. The pre-deploy gate catches
   empty-result queries and over-broad selectors before they hit
   production cardinality.
4. **Tune `alert_thresholds.score` per SLO** — the bundled defaults
   are conservative; verifying against `make backtest-inject` is the
   recommended path.
5. **Pin the image tag** — `image.tag: v1.0.0` in `values.yaml`. Do
   not float `latest`.

See [`docs/operations.md`](../../docs/operations.md) for the full
operational story, including the failover timeline and the sizing
table.
