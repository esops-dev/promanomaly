# Detectors

Every detector is a stateless scorer. It looks at a rolling window of recent samples for one time series and returns an `anomaly_score` for the newest sample plus a `baseline` value you can overlay on dashboards.

Sensitivity is controlled globally with `alert_thresholds.score` (default `3.0`). No per-detector tuning knobs.

For “which detector should I use?”, see [`decision-tree.md`](decision-tree.md).

## Common Output Metrics

- **`anomaly_score{...}`** — raw score (for MAD/Hampel this is in “MADs from baseline”)
- **`anomaly_outside_threshold{...}`** — `1` when `anomaly_score >= alert_thresholds.score`
- **`anomaly_baseline{...}`** — the detector’s normal-value estimate

## MAD — Median Absolute Deviation

```yaml
detectors:
  - name: MAD
```

**Best for**  
Noisy, heavy-tailed, or non-Gaussian signals: p99 latency, error rates, queue depth, throughput.

**Strengths**  
Extremely robust to outliers by construction — one bad point cannot pull the baseline.

**Limitations**  
- Slowly drifting baselines inside the window → use **Hampel**  
- Step changes that have already filled most of the window

**Parameters**  
None (window size is set globally via `defaults.window`).

## Hampel — Local-window Robust Outlier Filter

```yaml
detectors:
  - name: Hampel
    params:
      t0: 3        # default
```

**Best for**  
Same signals as MAD, but when the baseline drifts slowly within the rolling window.

**How it works**  
Computes median and MAD over a small local sub-window of `2*t0 + 1` points around the current sample.

**Parameters**  
- `t0` (default `3`) — half-width of the local sub-window. Larger values behave more like classical MAD; smaller values track drift faster.

### MAD vs Hampel

| Aspect                  | MAD                          | Hampel                          |
|-------------------------|------------------------------|---------------------------------|
| Baseline stable         | Preferred                    | Good                            |
| Baseline drifts         | Over-alerts                  | Preferred                       |
| Speed                   | Faster                       | Slightly slower                 |
| Tuning                  | Threshold only               | `t0` + threshold                |

## Adding a New Detector (for contributors)

1. Implement the `Detector` protocol in `detector/src/promanomaly/detectors/<name>.py`.  
2. Register it via the `promanomaly.detectors` entry point in `detector/pyproject.toml`.  
3. Add tests using the fixtures in `detector/tests/fixtures.py`.  
4. Document it here (description + defaults).

CI keeps code, registration, and docs in sync.