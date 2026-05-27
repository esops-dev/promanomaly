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

## ZScoreEWMA — Exponentially Weighted Z-Score

```yaml
detectors:
  - name: ZScoreEWMA
    params:
      alpha: 0.05      # default
```

**Best for**
Roughly Gaussian signals with a stable but slowly drifting mean: cache hit rate, sustained throughput, slowly growing capacity.

**How it works**
Maintains an EWMA mean and EWMA variance over the rolling window. Score is the classic z-score: `|y - ewma_mean| / sqrt(ewma_variance)`. The EWMA tracks slow drift natively, so a regression that creeps in over hours does not pollute the baseline the way a fixed-window mean would.

**Counter-example — do not use on heavy-tailed traffic**
On p99 latency or request rates with fat tails, the variance estimator inflates after the first big spike, the z-score collapses, and you stop catching subsequent anomalies. MAD or Hampel are the right tools for those.

**Parameters**
- `alpha` (default `0.05`) — EWMA smoothing factor. Smaller values track the mean more slowly; larger values adapt faster but let recent anomalies pollute the baseline more.

## IQR — Tukey Interquartile Range

```yaml
detectors:
  - name: IQR
    params:
      k: 1.5           # default
```

**Best for**
Non-parametric detection with a sensitivity profile that differs from MAD — lighter on the tails, more weight on the middle 50% of the window. Pairs well with MAD inside an ensemble because the two disagree in interesting ways on heavy-tailed signals.

**How it works**
Computes the first and third quartiles (`Q1`, `Q3`) and flags samples outside `[Q1 - k*IQR, Q3 + k*IQR]`. Score is the normalised distance outside the band, in IQR units.

**Parameters**
- `k` (default `1.5`) — multiplier for the IQR band. `1.5` is Tukey's classical "outlier" definition; `3.0` is "far outlier".

### MAD vs IQR sensitivity

| Aspect                            | MAD                       | IQR (k=1.5)                 |
|-----------------------------------|---------------------------|-----------------------------|
| Heavy-tailed signals              | More tolerant             | Less tolerant               |
| Symmetric noise                   | Slightly higher precision | Slightly higher recall      |
| Reaction to a single outlier      | Quiet (robust)            | Slightly louder             |
| Pairs in ensemble                 | Yes (see `voting`)        | Yes                         |

## HourOfDayMAD — Stratified by Hour-of-Day

```yaml
detectors:
  - name: HourOfDayMAD
    params:
      lookback: 7d         # default
      bucket_width: 1h     # default
```

**Best for**
Signals with strong diurnal patterns where a fixed-window MAD smears the daily shape and either false-positives every off-peak hour or false-negatives genuine anomalies. The canonical case: "Tuesday 3am looks unlike prior 3am samples."

**This is still baseline detection, not forecasting.**
The detector scores the present against historical observations from the same hour-of-day bucket. It never predicts future values — that's promforecast's job.

**How it works**
Bucket every sample in the lookback window by its UTC hour-of-day (24 buckets by default). For the latest sample, compute the median and MAD across the same-bucket history and score it the same way the standard MAD detector does. The runner automatically expands the per-query fetch window when HourOfDayMAD is configured, so it composes with short-window detectors (MAD / Hampel) in the same query without forcing the group's `defaults.window` to grow.

**Parameters**
- `lookback` (default `7d`) — how far back to fetch baseline samples.
- `bucket_width` (default `1h`) — must divide 24h evenly (e.g. `30m`, `1h`, `2h`, `4h`).

**Resource note**
A long lookback against a large query can pull a lot of data; respect `safety.max_series_per_query` and `safety.max_total_series`. The full day-of-week / multi-week version (with bounded sliding-window query strategy) lands in a later milestone.

## BOCPD — Bayesian Online Change-Point Detection

```yaml
detectors:
  - name: BOCPD
    params:
      hazard: 250        # default
      threshold: 0.5     # default
      lag: 15            # default
```

**Best for**
Detecting regime shifts on signals where you want a probabilistic change-point score — deploys, rolling restarts, autoscaler steps, anything that breaks the baseline rather than spiking through it.

**How it works**
Models the observation stream with a Normal-Gamma conjugate prior and maintains a posterior distribution over the *run length* (samples since the last change-point). The score is the posterior mass at recent run lengths (`r_t <= lag`) — high when a change-point happened in the last `lag` samples. Fires when score ≥ `threshold`; `threshold` is a per-detector override of `alert_thresholds.score` because the score is a probability, not a sigma multiplier.

**Parameters**
- `hazard` (default `250`) — expected interval between change-points in samples.
- `threshold` (default `0.5`) — posterior probability above which the detector fires.
- `lag` (default `15`) — inclusive run-length window counted as "recent".

**Trade-offs**
Probabilistic and confidence-gateable; more expensive than CUSUM (`O(window²)` per fit, still cheap in absolute terms); assumes Gaussian-ish observations.

## CUSUM — Cumulative Sum

```yaml
detectors:
  - name: CUSUM
    params:
      k: 0.5      # default
      h: 5.0      # default
```

**Best for**
A fast, interpretable change-point detector. Workhorse of industrial process control; pairs well with BOCPD when you want both probabilistic and classical signals on the same metric.

**How it works**
Tracks running upper/lower cumulative sums of `(x - median - k*sigma)` against a MAD-derived sigma estimate. A change-point fires on a *fresh* crossing of `h*sigma` at the latest sample (no double-counting on overlapping rolling-window calls). The score is `max(|S+|, |S-|) / sigma`, in sigma units — same scale as MAD/Hampel.

**Parameters**
- `k` (default `0.5`) — reference value (in sigma units); slack subtracted before accumulation.
- `h` (default `5.0`) — decision threshold (in sigma units).

**Trade-offs**
Single-pass, `O(window)`; intuitive sigma-multiplier units; less natural for non-Gaussian likelihoods but the MAD-derived sigma helps on heavy-tailed signals.

### When to choose BOCPD vs CUSUM

| Aspect                       | BOCPD                                | CUSUM                               |
|------------------------------|--------------------------------------|-------------------------------------|
| Output units                 | Probability `[0, 1]`                 | Sigma multiplier                    |
| Tuning surface               | `hazard` + `threshold` + `lag`       | `k` + `h`                           |
| Per-fit cost                 | `O(window²)`                         | `O(window)`                         |
| Probabilistic interpretation | Yes                                  | No                                  |
| Best when                    | You want a confidence-gated alert    | You want simple, fast, time-tested  |

If unsure: start with CUSUM, layer BOCPD when you want posterior-confidence gating. See [`change-points.md`](change-points.md) for worked examples (synthetic step change, deploy-induced regression).

## Auto-select Best Detector Per Series

```yaml
groups:
  - name: my_group
    auto_select: true            # or "explicit_plus_best"
    auto_select_interval: 24h    # default
    queries:
      - id: requests
        promql: ...
        detectors:
          - name: MAD
          - name: Hampel
          - name: IQR
```

When `auto_select: true`, the runner periodically injects three canonical synthetic anomalies (point spike, step change, level drift) into each series's rolling window and measures every configured detector's F1. The winner's score samples are emitted with an extra `best_detector=<name>` label; the losers are suppressed. `auto_select: "explicit_plus_best"` keeps every detector's output and only tags the winner — useful while tuning.

The full algorithm and tuning knobs live in [`auto-select.md`](auto-select.md).

## Ensemble Fusion (max / voting)

When a query runs more than one detector, set a group-level `ensemble:` block to publish a single composite verdict instead of writing recording rules to deduplicate:

```yaml
groups:
  - name: latency_alerts
    ensemble:
      method: voting       # max | voting
      min_detectors: 2     # only used when method: voting
    queries:
      - id: p95_latency
        promql: ...
        detectors:
          - name: MAD
          - name: Hampel
          - name: IQR
```

Two metrics appear only when `ensemble:` is configured (so configs without it stay unchanged):

- `anomaly_composite_score{id, group, <source labels>}` — max of per-detector raw scores.
- `anomaly_composite_outside_threshold{id, group, <source labels>}` — `1` when the chosen method fires.

`max` is logical-OR across detectors. `voting` fires only when at least `min_detectors` agree — the case that's awkward to express as a single recording rule, which is why ensemble is a first-class feature here.

`auto_select` and `ensemble:` compose: even when `auto_select` suppresses losers from `/metrics`, the ensemble fuser still sees every configured detector's verdict so voting matches what the config declared.

Weighted voting, Bayesian fusion, and ML-trained combiners are intentionally **out of scope**. Operators who want them compose recording rules; the kept-simple ensemble surface is part of the project's stateless promise.

See [`operations.md`](operations.md) for the worked example.

## Per-detector Confidence and Baseline Stability

When calibration is enabled (either by setting `auto_select: true` or by leaving the default `defaults.emit_baseline_stability: true` in place), each detector emits two extra gauges:

- `anomaly_confidence_score{id,group,detector,...}` — 0–1 trust signal.
- `anomaly_baseline_stability{id,group,detector,...}` — 0–1 stability of the rolling baseline.

Use them to gate alerts before paging:

```promql
anomaly_outside_threshold == 1
  and on (id, group, detector) anomaly_confidence_score > 0.7
  and on (id, group, detector) anomaly_baseline_stability > 0.6
```

The exact formulas live in [`confidence.md`](confidence.md).

## Per-Detector Hyperparameter Overrides

Two forms are accepted under `detectors:`:

```yaml
detectors:
  - MAD                                 # string shorthand, no overrides
  - name: Hampel                        # dict form, optional params
    params:
      t0: 5
  - name: MAD                           # multiple instances of one detector
    instance: short                     # need a unique 'instance:' each
    params:
      window: 5m
  - name: MAD
    instance: long
    params:
      window: 1h
```

`params:` is forwarded to the detector in two complementary ways:

1. **As constructor kwargs** when the detector's `__init__` declares matching keyword arguments (or accepts `**kwargs`). Unrecognised keys are dropped silently so the runner can grow new defaults without breaking detectors that don't yet use them.
2. **As the third argument to `fit_score(df, freq, params)`** — the legacy shape, kept for backwards compatibility.

Built-ins consume params via `fit_score`. Community detectors that prefer to stash overrides at construction time get the same surface — no fork required. The `instance:` field is required when the same `name:` appears more than once in a query; the schema rejects duplicate names without it.

## Adding a New Detector (for contributors)

1. Implement the `Detector` protocol in `detector/src/promanomaly/detectors/<name>.py`.  
2. Register it via the `promanomaly.detectors` entry point in `detector/pyproject.toml`.  
3. Add tests using the fixtures in `detector/tests/fixtures.py`.  
4. Document it here (description + defaults).

CI keeps code, registration, and docs in sync.