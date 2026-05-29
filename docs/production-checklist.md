# Production-Readiness Checklist & Sizing Guide

This single page answers the questions every operator must resolve before going live: how much CPU/memory to request, which deployment shape to choose, and which knobs to verify.

Detailed guidance lives in the linked pages; this is the integrated checklist.

## Sizing (Rule of Thumb)

Detection is cheap (`O(n log n)` per series on the rolling window). The bottleneck is **TSDB query throughput**, not compute.

A single 1-CPU replica comfortably handles **~10 000 series at 1-minute refresh** with default detectors.

| Series count     | Refresh | CPU   | Memory | Replicas          |
|------------------|---------|-------|--------|-------------------|
| < 1 000          | 1 m     | 100m  | 128 Mi | 1 (no HA)         |
| 1 000 – 10 000   | 1 m     | 1     | 512 Mi | 1 or 2 (HA)       |
| 10 000 – 25 000  | 1 m     | 2     | 1 Gi   | 2 (HA)            |
| 25 000 – 50 000  | 2 m     | 4     | 2 Gi   | 2–3 (HA)          |
| > 50 000         | varies  | —     | —      | Shard by group    |

**Always run** `promanomaly validate --config <file> --estimate-cost` for a config-specific projection (projected series, TSDB queries per refresh, and coarse CPU/memory estimate).

**Refresh-interval impact**  
CPU scales linearly with `1 / refresh_interval`. Keep `refresh_interval` no smaller than `defaults.window / 6` (the detector warns on aggressive settings).

**Stratified detectors** add TSDB reads (not CPU). The cost estimator reports `stratified_baseline_queries_per_day` separately.

## Reference Architectures

| Shape                      | When to use                                      | Helm values |
|----------------------------|--------------------------------------------------|-------------|
| **A. Single-replica Recreate** | < 10 k series, brief `/metrics` gap OK during deploys | `replicaCount: 1`<br>`strategy: Recreate`<br>`highAvailability.enabled: false` |
| **B. HA leader-elected**   | Need continuous `/metrics` availability          | `replicaCount: 2`<br>`highAvailability.enabled: true`<br>`safety.redis.url: …` |
| **C. Multi-cluster**       | Many clusters (per-cluster or federated)         | See [multi-cluster reference architectures](architecture/multi-cluster.md) |

## Production-Readiness Checklist

| Item | Expected end state | Reference |
|------|--------------------|---------|
| Cardinality caps set | `safety.max_series_per_query` and `safety.max_total_series` sized for your fleet; `--estimate-cost --strict` passes in CI | [config schema](config-schema.md) |
| Source-failure policy chosen | `safety.on_source_failure` deliberately set (`serve_stale` is safe default) | [degraded modes](operations/degraded-modes.md) |
| Queries validated | `promanomaly validate --config <file> --probe --strict` passes against real TSDB | [CLI](cli.md) |
| Cost estimate run | `promanomaly validate --config <file> --estimate-cost --strict` passes | [CLI](cli.md) |
| ServiceMonitor present | promanomaly’s own `/metrics` is scraped | chart `values.yaml` |
| `honor_labels: true` on scrape | Source labels pass through unchanged | [LABELS_CONTRACT.md](LABELS_CONTRACT.md) |
| NetworkPolicy reviewed | `/-/reload` and `/debug/*` denied by default | chart `NetworkPolicy` |
| Alerts loaded | Shipped `promanomaly-rules.yaml` deployed (including self-observability) | [examples/alerts/](examples/alerts/promanomaly-rules.yaml) |
| Dashboards provisioned | Shipped Grafana dashboards installed | [dashboards/grafana/](dashboards/grafana/) |
| Degraded-mode alerts wired | `AnomalySourceFailing`, `AnomalyStale`, etc. on-call | [degraded modes](operations/degraded-modes.md) |
| Warm-up understood | `promanomaly warmup --target <url>` explains empty dashboards on first install | [CLI](cli.md) |

## What You Can Defer

- HA mode (single-replica `Recreate` is fully supported)
- `/warmup` endpoint (leave `expose_warmup_endpoint: false` after initial rollout)
- Stratified detectors (add later for diurnal signals)

## What You Cannot Defer

- `safety.max_total_series` (re-evaluate after every new detector or `discover:` block)
- Choosing a `safety.on_source_failure` policy
- Wiring the degraded-mode alerts

## Cross-References

- [Degraded-mode operations playbook](operations/degraded-modes.md)  
- [Multi-cluster reference architectures](architecture/multi-cluster.md)  
- [Operations guide](operations.md)  
- [Configuration schema](config-schema.md)  
- [CLI reference](cli.md)

Run the checklist, run the two `validate` commands, and you’re ready for production.