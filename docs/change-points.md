# Change-Point Detection

Change-point detectors fire **the instant** a metric’s underlying process shifts — a deploy changes the error-rate baseline, an autoscaler steps CPU, a feature flag doubles traffic.  

MAD/Hampel wait until the new regime fills the rolling window. Change-point detectors alert on the transition itself.

## When to Use Them

**Reach for BOCPD or CUSUM when:**
- Your metric breaks on deploys, rolling restarts, scale events, or feature flags.
- You want alerts in seconds, not after a `for:` window.
- promforecast bands smear the shift instead of catching it.

**Stick with MAD / Hampel / IQR when:**
- You’re hunting single noisy spikes (not sustained level shifts).
- The signal is heavy-tailed or sparse.

The two classes are complementary — many teams run both on the same query.

## Output Metrics

All change-point detectors emit the same family:

- `anomaly_change_point_total{...}` — counter that increments once per fresh change-point.
- `anomaly_score{...}` — gauge (in detector-specific units).
- `anomaly_outside_threshold{...}` — `1` when the detector fires.
- `anomaly_baseline{...}` — the detector’s current baseline.

## BOCPD — Bayesian Online Change-Point Detection

```yaml
detectors:
  - name: BOCPD
    params:
      hazard: 250      # default
      threshold: 0.5   # default
      lag: 15          # default
```

**Best for**  
Probabilistic alerts where you want to gate on confidence.

**How it works**  
Maintains a posterior over “how long since the last change-point” using a Normal-Gamma prior.  
Score = probability that the current run started in the last `lag` samples. Fires when score ≥ `threshold`.

**Parameters**

| Param       | Default | Meaning                                      |
|-------------|---------|----------------------------------------------|
| `hazard`    | 250     | Expected samples between change-points       |
| `threshold` | 0.5     | Posterior probability to fire                |
| `lag`       | 15      | Window size for “recent change”              |

## CUSUM — Cumulative Sum Control Chart

```yaml
detectors:
  - name: CUSUM
    params:
      k: 0.5     # default
      h: 5.0     # default
```

**Best for**  
Fast, simple, interpretable change detection.

**How it works**  
Two-sided cumulative sum against a robust (MAD-derived) baseline. Fires when either sum crosses `h` sigma. Score is in sigma units, same scale as MAD/Hampel.

**Parameters**

| Param | Default | Meaning                              |
|-------|---------|--------------------------------------|
| `k`   | 0.5     | Slack (in sigma)                     |
| `h`   | 5.0     | Decision threshold (in sigma)        |

## BOCPD vs CUSUM

| Aspect               | BOCPD                          | CUSUM                          |
|----------------------|--------------------------------|--------------------------------|
| Output               | Probability (0–1)              | Sigma multiplier               |
| Tuning               | `hazard`, `threshold`, `lag`   | `k`, `h`                       |
| Speed                | Slower (`O(window²)`)          | Faster (`O(window)`)           |
| Heavy-tailed data    | Weaker                         | Good (uses MAD sigma)          |
| Best when            | You want confidence gating     | You want simple & fast         |

**Recommendation:** Start with **CUSUM**. Add BOCPD alongside if you want probabilistic gating.

## Quick Example Alert

```yaml
- alert: ErrorBaselineShifted
  expr: |
    increase(anomaly_change_point_total{id="error_rate", detector="BOCPD"}[10m]) > 0
    and on (id, group)
    anomaly_outside_threshold{id="error_rate", detector="BOCPD"} == 1
  for: 2m
```

## Offline Analysis

```bash
promanomaly analyze \
  --query 'rate(http_requests_total{job="api"}[5m])' \
  --from 2026-05-25T00:00:00Z \
  --to 2026-05-25T06:00:00Z \
  --detector ruptures
```

See `cli.md` for full options.

For more composition patterns (rate-of-change input, group rollups, deploy correlation) see the [Composition Patterns Cookbook](patterns.md).