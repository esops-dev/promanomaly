# When to Use promforecast, promanomaly, or Both

The two tools are **complementary**, not overlapping. This guide helps you pick the right one (or both) for any signal before you start tuning.

For “which detector inside promanomaly?” see [`decision-tree.md`](decision-tree.md).  
For composition recipes (rate-of-change, multi-window, cross-tool joins) see [`patterns.md`](patterns.md).

## One-Line Summary

- **promforecast** answers: *“What will this metric look like in N hours?”*
- **promanomaly** answers: *“Is this metric behaving abnormally right now?”*

## At a Glance

| Aspect                    | promforecast                                      | promanomaly                                          |
|---------------------------|---------------------------------------------------|------------------------------------------------------|
| Core question             | “What will this be in N hours?”                   | “Is this abnormal right now?”                        |
| Approach                  | Long-lookback model + forward prediction          | Rolling-window baseline scoring                      |
| Lookback                  | Days to weeks                                     | Minutes to hours (stratified detectors use days+)    |
| Detection latency         | Minutes to hours                                  | Seconds to minutes                                   |
| Best for                  | Capacity, traffic, seasonal KPIs                  | Bursty, sparse, step-changing, fleet-relative        |
| Handles step changes      | Slowly (smears into next refit)                   | Immediately (BOCPD / CUSUM)                          |
| Catches “one bad replica” | No                                                | Yes (Cohort detector)                                |
| Output metrics            | `*_forecast{…}`, bands, ETAs                      | `anomaly_score{…}`, `anomaly_outside_threshold{…}`  |
| Compute cost              | Heavier per series                                | Lightweight                                          |

Both tools share the exact same `(id, group)` label contract, so you can join their outputs in PromQL with zero rewriting.

## Decision Tree

```
Start here
│
├── Is the question “what will this metric look like in N hours”?
│   ├── Yes → promforecast
│   └── No  → continue
│
├── Does the signal have clear hourly/daily seasonality AND you need multi-hour forecasts?
│   ├── Yes → promforecast
│   └── No  → continue
│
├── Does the signal step-change on deploys, scale events, or feature flags?
│   ├── Yes → promanomaly (immediate change-point detection)
│   └── No  → continue
│
├── Is the anomaly “the signal completely stopped reporting”?
│   ├── Yes → promanomaly (`expect:` or dynamic absence)
│   └── No  → continue
│
├── Is the anomaly “one member of a fleet looks different from the rest”?
│   ├── Yes → promanomaly (Cohort detector)
│   └── No  → continue
│
├── Is the signal sparse, bursty, or heavy-tailed (p99 latency, rare errors, queue depth)?
│   ├── Yes → promanomaly (MAD / Hampel / IQR)
│   └── No  → continue
│
└── Reached here?
    └── Smooth seasonal capacity/traffic metric → **both tools** work.
        promforecast gives the long-horizon view; add promanomaly for fast break detection.
```

## When to Run Both

There is **no overlap** to deduplicate. They catch different failure modes on the same signal:

- promforecast → slow capacity creep, forecast-band breaches
- promanomaly → sudden step changes, one-bad-replica cases, sparse-event spikes

They feed independent `PrometheusRule` files into the same Alertmanager. Use `inhibition_rules` keyed on `(id, group)` to suppress duplicate pages when both fire on the same outage.

**Worked example** (B2B API with 50 replicas):
- promforecast on total request rate → capacity planning + ETAs
- promanomaly on the same rate (MAD + ZScoreEWMA) → fast deploy detection
- promanomaly Cohort on per-replica rate → “one bad replica” alerts
- Recording rule joins both on `(id, group)` for a single “API health” rollup

## When to Pick Only One

- **promforecast only**  
  Clean seasonal capacity/traffic metrics where long-horizon forecasts matter and you don’t need instant step-change or per-instance detection.

- **promanomaly only**  
  Bursty/sparse signals, frequent regime shifts, or fleets where the canonical anomaly is “one member is different.”

For almost everything else: **run both**.

## What Neither Tool Does

- Alerting or routing (that’s Alertmanager)
- Dashboards or UI (that’s Grafana)
- Causal analysis (“did the deploy cause this?”)
- Log/trace anomaly detection
- Full multivariate covariance across unrelated signals

All advanced patterns are handled with standard PromQL, recording rules, or Alertmanager silences — see [`patterns.md`](patterns.md).