# Choosing a Detector

This short guide helps you answer two questions fast:  
**“Should I use promforecast or promanomaly?”** and **“Which detector inside promanomaly fits my signal?”**

It is written so you can pick the right tool on day one instead of tuning the wrong one for days.

### Step 1 — promforecast vs. promanomaly

```
Is the question “what will this metric look like in N hours”?
├── Yes → promforecast (forecast bands, ETAs, capacity warnings)
└── No  → promanomaly

Does the signal have clear hourly/daily seasonality + you care about long horizons?
├── Yes → promforecast
└── No  → promanomaly

Does the signal jump on deploys, scale events, or feature flags?
├── Yes → promanomaly (change-point detectors catch shifts instantly)
└── No  → either tool works

Is the anomaly “the signal is completely missing” (heartbeats, cron jobs)?
└── Yes → promanomaly (`expect:` flag or planned dynamic absence detection)

Quick rule: run both when it makes sense — promforecast for capacity, promanomaly for error rates, sparse events, and per-instance outliers.
```

### Step 2 — Which detector inside promanomaly?

```
Noisy, heavy-tailed, or non-Gaussian (p99 latency, error rates, queue depth, throughput)?
├── Yes → MAD (default) or Hampel
│   ├── Stable baseline in the window → MAD
│   └── Drifting baseline          → Hampel
└── No

Roughly Gaussian with slowly drifting mean (cache hit rate, sustained throughput)?
└── Yes → ZScoreEWMA (planned)

Counter with mostly-zero gaps (sparse errors, rare events)?
└── Yes → MAD + longer window (pair with `min_abs_delta` floor when available)

Strong daily/hourly pattern (request rate, batch counters)?
└── Yes → HourOfDayMAD (planned)

Breaks on deploys or rolling restarts?
└── Yes → BOCPD / CUSUM (planned change-point detectors). Until then use short-window MAD.

Comparing identical instances (nodes, pods, replicas)?
└── Yes → Cohort detector (planned)
```

### Quick Cross-Checks Before Shipping

1. **Sensitivity**  
   Set once with `alert_thresholds.score` (default `3.0`).  
   Tighten to `4.0` for fewer false positives, loosen to `2.5` when missing anomalies hurts more.

2. **Warm-up**  
   Default: `emit_warming_up` (dashboards see a flag).  
   Use `suppress` if warm-up labels are noisy.

3. **Composition**  
   Rate-of-change, acceleration, etc. → just wrap your query with `rate()` / `deriv()` (see `docs/patterns.md`).

### When the Tree Gives the Wrong Answer

Run a quick test:

```bash
promanomaly detect-once --query '<your-promql>' --detector <name>
```

Look at the score vs. the actual samples.  
If it feels wrong, start with **MAD**, then try **Hampel**, then the upcoming change-point detectors.
