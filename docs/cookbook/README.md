# Operator Cookbook

Copy-paste ready configurations for the metrics you actually monitor. Each recipe includes the right detector, sensible defaults, and the exact trade-offs so you can ship on day one instead of guessing.

Every config block in this cookbook is **schema-validated** in CI — copy it, paste it, and it will work.

## Recipes by Domain

- **[Infrastructure](infrastructure.md)** — node filesystem fill, CPU saturation, memory pressure, disk I/O latency, network errors
- **[Kubernetes](kubernetes.md)** — pod restart counters, container CPU throttling
- **[Applications & Middleware](applications.md)** — Kafka consumer lag, HTTP 5xx rate, HTTP p99 latency, certificate/heartbeat absence

## Detector Quick Reference

| Signal shape                          | Recommended detector          | Why it fits |
|---------------------------------------|-------------------------------|-------------|
| Noisy, heavy-tailed, stable baseline  | `MAD`                         | Robust, no distributional assumptions |
| Baseline drifts inside window         | `Hampel`                      | Local sub-window tracks drift |
| Roughly Gaussian + slow drift         | `ZScoreEWMA`                  | Native drift handling |
| Step changes / regime shifts          | `CUSUM` or `BOCPD`            | Immediate change-point detection |
| One bad member in a fleet             | `Cohort`                      | Cross-series comparison |
| Strong daily/weekly pattern           | `HourOfDayMAD` / `DayOfWeekMAD` | Same-bucket historical baseline |
| Absence of the signal is the anomaly  | `expect: true`                | Heartbeat / scheduled-job detection |

Full decision tree → [`decision-tree.md`](../decision-tree.md)  
Detector reference → [`detectors.md`](../detectors.md)

## Built-in Operator Signals

Leave these at their defaults:

- `defaults.emit_severity: true` → gives you a normalized 0–1 `anomaly_severity` gauge for instant ranking across all detectors (see [severity.md](../severity.md))
- Use `promanomaly top` for live triage without writing PromQL (see [triage.md](../triage.md))

Each recipe below also includes:
- Why the signal matters
- Expected dashboard behaviour
- Cardinality impact
- Ready-to-use PrometheusRule example

Start with the domain that matches your stack, copy the block, run `promanomaly validate --probe`, and you’re done.