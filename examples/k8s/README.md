# Reaction recipes: wiring anomaly signal into KEDA / HPA / Argo Rollouts

Worked examples for feeding promanomaly's anomaly signal to the systems
that *act* on it — autoscalers and rollout controllers — with the
guard-rails baked in. promanomaly only emits signal; every decision here
is made by the reaction system, not the detector.

| File | System | Pattern |
| --- | --- | --- |
| [`keda-scaledobject.yaml`](keda-scaledobject.yaml) | KEDA | Scale workers on anomalous backlog depth. |
| [`hpa-custom-metrics.yaml`](hpa-custom-metrics.yaml) | HPA | Same, via the external-metrics adapter. |
| [`argo-rollouts-analysistemplate.yaml`](argo-rollouts-analysistemplate.yaml) | Argo Rollouts | Cohort-gated canary: abort when the canary diverges from its peers. |

## The guard-rails (why they're in every file)

Anomaly signal is a *sharp* input. Wire it carelessly and it amplifies
incidents. Every recipe encodes these rules:

1. **Scale on density, not score.** `anomaly_density` ("how much of the
   backlog is anomalously deep") is a sound scaling input.
   `anomaly_score` is **not** — scaling on how anomalous a latency metric
   looks creates a feedback loop where the spike scales you, and the
   scaling deepens the spike.
2. **Gate on confidence and duration.** React to sustained,
   high-confidence anomalies (`anomaly_severity_density`,
   `anomaly_duration_seconds`, `count`/`interval`), never a single tick.
3. **Always keep a reactive fallback.** Pair the anomaly-driven trigger
   with a plain reactive one (queue length, CPU). If the detector or TSDB
   is down, the fallback keeps the workload safe — the anomaly path is
   never the only thing between you and an unbounded queue.
4. **Cap the blast radius.** `minReplicaCount`/`maxReplicaCount` floors
   and ceilings, long scale-down stabilization windows.

## Which path needs the metrics adapter?

- **KEDA** and **Argo Rollouts** talk to the TSDB's Prometheus HTTP API
  directly (`prometheus` scaler / metric provider) — no adapter required.
- **HPA** consumes Kubernetes metrics APIs, so the
  [`promanomaly-metrics-adapter`](../../charts/promanomaly-metrics-adapter/)
  chart must be installed for `hpa-custom-metrics.yaml`.

## Why cohort gating for canaries is the standout

The Argo Rollouts recipe is the strongest fit. Static canary thresholds
("p95 < 300ms") need per-service tuning and drift. The Cohort detector
instead asks "does the canary look *different from its stable peers right
now?*" — which needs no threshold and adapts to whatever the service's
current normal is. A canary that regresses relative to the fleet fails
the rollout automatically.
