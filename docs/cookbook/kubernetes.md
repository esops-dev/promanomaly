# Kubernetes Signals

Recipes for kube-state-metrics and cAdvisor signals.  
The complete ready-to-use starters are in  
[`examples/configs/kube-state-metrics.yaml`](../../examples/configs/kube-state-metrics.yaml) and  
[`examples/configs/cadvisor.yaml`](../../examples/configs/cadvisor.yaml).

Both set `defaults.density_by: [namespace]` so the built-in fleet anomaly-density rollup tells you “which namespace is unhealthy?”.

---

## Pod-Restart Counters

**Why it matters.** A workload suddenly entering a crash loop is one of the clearest “something just broke” signals. The shape is a step change from zero restarts to a sustained high rate.

**Recommended detectors.** `MAD` for spike detection + `CUSUM` for the persistent regime shift. A `min_abs_delta` floor prevents noise on never-restarting pods.

```yaml
apiVersion: promanomaly.io/v1
datasource:
  url: http://victoriametrics:8428/
defaults:
  window: 1h
  step: 30s
  min_points: 30
  density_by:
    - namespace
groups:
  - name: kube_pod_restarts
    queries:
      - id: kube_pod_restart_rate
        promql: |
          sum by (namespace, pod) (
            rate(kube_pod_container_status_restarts_total[15m])
          )
        min_abs_delta: 0.001
        detectors:
          - name: MAD
          - name: CUSUM
```

**Dashboard shape.** Restart rate flat at zero, then a clean step; CUSUM marks the exact onset of the crash loop.

**Cardinality.** One series per `(namespace, pod)`. High-churn namespaces (Jobs, CI runners) can increase this — scope the PromQL or raise `max_series_per_query` after `validate --probe`.

**Example alert.**
```yaml
- alert: PodCrashLoopAnomaly
  expr: anomaly_outside_threshold{id="kube_pod_restart_rate"} == 1
  for: 5m
```

---

## Replica Availability / Rollout Stalls

**Why it matters.** A persistent gap between desired and available replicas means a rollout is stuck or pods cannot schedule.

**Recommended detectors.** `MAD` for general anomalies + `BOCPD` for the clean regime shift (from “all good” to “gap exists”).

```yaml
apiVersion: promanomaly.io/v1
datasource:
  url: http://victoriametrics:8428/
defaults:
  window: 1h
  step: 30s
  min_points: 30
groups:
  - name: kube_replica_gap
    queries:
      - id: kube_deployment_replica_gap
        promql: |
          kube_deployment_spec_replicas
          - kube_deployment_status_replicas_available
        detectors:
          - name: MAD
          - name: BOCPD
```

**Cardinality.** One series per `(namespace, deployment)`.

**Example alert.**
```yaml
- alert: DeploymentReplicaGapAnomaly
  expr: increase(anomaly_change_point_total{id="kube_deployment_replica_gap"}[10m]) > 0
  for: 5m
```

---

## Container CPU Throttling

**Why it matters.** A container hitting its CPU limit throttles silently, adding latency. The fleet case — one replica throttling due to bad node placement — is invisible to per-series detectors alone.

**Recommended detectors.** `MAD` per pod + `Cohort` across replicas for divergence detection.

```yaml
apiVersion: promanomaly.io/v1
datasource:
  url: http://victoriametrics:8428/
defaults:
  window: 1h
  step: 30s
  min_points: 30
  density_by:
    - namespace
groups:
  - name: container_cpu_throttling
    queries:
      - id: container_cpu_throttled_fraction
        promql: |
          sum by (namespace, pod) (rate(container_cpu_cfs_throttled_periods_total[5m]))
          / clamp_min(sum by (namespace, pod) (rate(container_cpu_cfs_periods_total[5m])), 1)
        min_abs_delta: 0.01
        detectors:
          - name: MAD
          - name: Cohort
            instance: fleet
            params:
              cohort_label: pod
              min_cohort_size: 5
```

**Dashboard shape.** Throttled fraction per pod; Cohort firings highlight the single replica diverging from its peers.

**Cardinality.** One series per `(namespace, pod)`.

**Example alert.**
```yaml
- alert: ContainerCPThrottlingAnomaly
  expr: anomaly_severity{id="container_cpu_throttled_fraction"} >= 0.7
  for: 10m
```