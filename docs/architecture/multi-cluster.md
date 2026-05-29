# Multi-Cluster Reference Architectures

Running promanomaly across many Kubernetes clusters raises one key question the single-cluster docs don’t answer: **where should the detector live relative to the workloads it watches, and where should its analytics-plane TSDB live?**

There is no single right answer — the trade-off is between cardinality, cross-cluster network cost, and blast radius/autonomy.

This page documents the three patterns that cover essentially every deployment. It mirrors promforecast’s multi-cluster guide so a platform team can deploy both tools the same way.

All patterns assume the standard flow: production Prometheus `remote_write`s into a long-term VictoriaMetrics (analytics plane), and the detector reads a short rolling window from that VM.

## At a Glance

| Pattern | Detectors                  | Analytics TSDB                  | Best when |
|---------|----------------------------|---------------------------------|-----------|
| **A. Per-cluster detector + central aggregation** | One per cluster            | Per-cluster VM → anomaly scores `remote_write` to central VM | You want per-cluster autonomy + one central anomaly view |
| **B. Central detector + federated VM** | One (or HA set) centrally  | One central/federated VM        | Clusters are small/homogeneous and central bandwidth is cheap |
| **C. Fully isolated per-cluster** | One per cluster            | Per-cluster VM (no aggregation) | Hard tenancy, data-residency, or regulatory isolation |

## Pattern A — Per-Cluster Detector with Central Aggregation (Recommended Default)

```
cluster-1: prod-prom ─► VM-1 ─► detector-1 ─┐
cluster-2: prod-prom ─► VM-2 ─► detector-2 ─┼─ remote_write anomaly_* ─► central VM ─► Grafana / Alertmanager
cluster-N: prod-prom ─► VM-N ─► detector-N ─┘
```

Each cluster runs its own detector against its local VM. Only the bounded anomaly output (`anomaly_*` metrics) crosses the boundary to a central VM (via `remote_write` or federated scrape). Add a `cluster` label upstream for slicing.

**Trade-offs**

- **Cardinality**: Best — each detector sees only its own cluster.
- **Network**: Low — only anomaly scores cross clusters.
- **Blast radius / autonomy**: Best — one cluster’s outage never affects others.
- **Operational cost**: N detector deployments (easy to template with GitOps).

**Use when** you have more than a handful of clusters and want both autonomy and a single pane of glass. This is the recommended default for growing fleets.

## Pattern B — Central Detector Reading a Federated VictoriaMetrics

```
cluster-1: prod-prom ─┐
cluster-2: prod-prom ─┼─ remote_write ─► central/federated VM ─► detector (HA) ─► Grafana / Alertmanager
cluster-N: prod-prom ─┘
```

All clusters write into one central VM. A single (usually HA) detector reads everything from it.

**Trade-offs**

- **Cardinality**: Worst — the detector must handle the entire fleet at once.
- **Network**: High — all PromQL reads hit the central VM.
- **Blast radius / autonomy**: Worst — central VM or detector outage blinds the whole fleet.
- **Operational cost**: Lowest — one config, one deployment.

**Use when** clusters are small/homogeneous, the central VM already exists for other reasons, and total cardinality comfortably fits one detector. Switch to Pattern A once you approach the single-replica sizing limit.

## Pattern C — Fully Isolated Per-Cluster Detectors

```
cluster-1: prod-prom ─► VM-1 ─► detector-1 ─► cluster-1 Grafana / Alertmanager
cluster-2: prod-prom ─► VM-2 ─► detector-2 ─► cluster-2 Grafana / Alertmanager
cluster-N: prod-prom ─► VM-N ─► detector-N ─► cluster-N Grafana / Alertmanager
```

Every cluster is completely self-contained. Nothing crosses boundaries.

**Trade-offs**

- **Cardinality**: Best (same as A).
- **Network**: None.
- **Blast radius / autonomy**: Best — total isolation.
- **Operational cost**: Highest (N of everything).

**Use when** hard multi-tenancy, data-residency, or regulatory rules forbid any cross-cluster dependency. If you need isolation for raw metrics but still want a central anomaly view, prefer Pattern A.

## Choosing Between Them

1. **Start with blast radius**: If clusters must be independent → A or C. If a central control plane is acceptable → consider B.
2. **Check cardinality**: If aggregate fleet exceeds one detector’s budget → B is out; use A.
3. **Decide on central view**: Want one pane of glass → A. Must not have one → C.

**When in doubt, start with Pattern A** — it gives you the best of both worlds: edge-local detection (great cardinality and blast radius) plus a central anomaly view, with only the already-bounded anomaly signal crossing the wire.

## Cross-References

- [Operations guide](../operations.md) — HA, `remote_write` sink, dynamic discovery
- [Production sizing & readiness checklist](../production-checklist.md)
- [Degraded-mode operations playbook](../operations/degraded-modes.md)