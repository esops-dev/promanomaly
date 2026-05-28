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
    params:
      recursive_trim: false   # default
      trim_threshold: 3.0     # default
```

**Best for**  
Noisy, heavy-tailed, or non-Gaussian signals: p99 latency, error rates, queue depth, throughput.

**Strengths**  
Extremely robust to outliers by construction — one bad point cannot pull the baseline.

**Limitations**  
- Slowly drifting baselines inside the window → use **Hampel**  
- Step changes that have already filled most of the window
- Clustered multi-point anomalies inside the rolling window pollute the median + MAD → enable `recursive_trim`

**Parameters**  
- `recursive_trim` (default `false`) — when true, run the baseline once, drop in-window points above `trim_threshold`, and recompute the median + MAD once more before scoring the latest sample. Bounded to a single trim pass for predictability. Mitigates the clustered-anomaly limitation: a burst of 4–5 anomalous points in the rolling window normally poisons the very median MAD is computed against, and the latest sample looks normal. With `recursive_trim` on, the burst is excluded from the recomputed baseline and the latest sample scores as the operator would expect.
- `trim_threshold` (default `3.0`) — MAD-units cutoff for the trim pass. Matches the conventional 3-MAD anomaly threshold and the default `alert_thresholds.score`. **Deliberately separate from `alert_thresholds.score`** — alerting policy can change without silently re-shaping the baseline. Raise it when the rolling window legitimately contains points beyond 3 MADs that you don't want stripped; lower it for very clean signals where 3 MADs is already noise.

  Worked example — `clustered_spikes(cluster=6, spike=10.0)`:
  - Without trim: baseline pulled toward the spike value, score ≈ 0.
  - With trim: baseline snaps back to the pre-burst mean, score well above 3.

  Window size is set globally via `defaults.window`.

## Hampel — Local-window Robust Outlier Filter

```yaml
detectors:
  - name: Hampel
    params:
      t0: 3                  # default
      recursive_trim: false  # default
      trim_threshold: 3.0    # default
```

**Best for**  
Same signals as MAD, but when the baseline drifts slowly within the rolling window.

**How it works**  
Computes median and MAD over a small local sub-window of `2*t0 + 1` points around the current sample.

**Parameters**  
- `t0` (default `3`) — half-width of the local sub-window. Larger values behave more like classical MAD; smaller values track drift faster.
- `recursive_trim` (default `false`) — when true, recompute the local median + MAD once after excluding sub-window points above `trim_threshold` from the first pass. Same single-iteration bound as MAD. Particularly useful for Hampel because the local sub-window is small: a 4-point cluster right at the tail of the window otherwise fills most of it and the latest sample looks normal.
- `trim_threshold` (default `3.0`) — local-MAD-units cutoff for the trim pass; same default and decoupling rationale as MAD's `trim_threshold`.

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

**Time zone caveat**
Buckets are computed against **UTC** wall-clock time because Prometheus stores UTC. If your operations team thinks in a non-UTC timezone (e.g. JST = UTC+9), "9 AM local" buckets line up with UTC midnight bucket; bucket boundaries don't align with local "hour of day". This is fine for periodic anomaly detection but can confuse on-call ("the alert says hour 0 was anomalous; it's 9 AM here"). A local-timezone option is deferred until enough operators ask for it — for now, document the offset alongside your runbook or use a `bucket_width` that's a divisor of your timezone offset to keep buckets aligned.

**Resource note**
A long lookback against a large query can pull a lot of data; respect `safety.max_series_per_query` and `safety.max_total_series`. The full day-of-week / multi-week version is `DayOfWeekMAD` (below) and uses the runner's sliding-window fetch strategy to keep TSDB load bounded.

## DayOfWeekMAD — Stratified by (Day-of-Week, Hour-of-Day)

```yaml
detectors:
  - name: DayOfWeekMAD
    params:
      lookback: 4w                    # default
      bucket_width: 1h                # default
      baseline_refresh_interval: 1h   # default
```

**Best for**
Weekly-periodic signals where weekend traffic behaves nothing like weekday traffic — B2B request rates that go quiet on Saturdays, batch jobs that only run on Mondays, business-hours error rates that differ from overnight. A single `HourOfDayMAD` median-collapses weekday + weekend samples and either smears the baseline or false-positives every weekend afternoon; `DayOfWeekMAD` keeps them separate via a 7x24 grid.

**This is still baseline detection, not forecasting.**
Same carve-out as `HourOfDayMAD`. The detector scores the present against historical observations from the same `(day-of-week, hour-of-day)` bucket — it never predicts future values.

**How it works**
Bucket every sample in the lookback window by `(weekday(), hour-of-day)` (default 7x24 grid, all in UTC). For the latest sample, find its bucket, compute median + MAD over the same-bucket history, and score `|y - bucket_median| / bucket_MAD`. The `weekday()` value follows Python's `datetime.weekday()` convention (Monday=0).

**Parameters**
- `lookback` (default `4w`) — multi-week window. Sized in weeks so every bucket has several observations to median over. Accepts `w` (weeks), `d` (days), `h` (hours), `m` (minutes).
- `bucket_width` (default `1h`) — within-day bucket width; must divide 24h evenly.
- `baseline_refresh_interval` (default `1h`) — opts the detector into the runner's sliding-window fetch strategy: the heavy multi-week query runs at this cadence rather than every `server.refresh_interval`. See [`scaling-stratified.md`](scaling-stratified.md) for the operational rationale.

**Time zone caveat**
Both `weekday()` and the within-day bucket are computed in **UTC**. For operators in non-UTC timezones the weekday boundary lands at local times other than midnight: in JST (UTC+9), the UTC "Sunday" bucket covers Sunday 09:00 → Monday 09:00 local. That's fine for weekly periodicity detection but operators reading dashboards should remember that "weekday 5 (Saturday)" in a metric label refers to UTC Saturday, not local Saturday. Same deferred-timezone-option rationale as `HourOfDayMAD`.

**When to choose**
- Use `DayOfWeekMAD` when weekday vs. weekend (or Monday-vs-rest) behaviour materially differs.
- Use `HourOfDayMAD` when the daily shape is consistent across the week and a one-week lookback is enough.
- Use plain `MAD` when neither dimension is informative — the rolling window already captures the relevant context.

**Example**
[`examples/configs/weekly-baseline.yaml`](../examples/configs/weekly-baseline.yaml) ships a production-shaped reference combining `HourOfDayMAD` + `DayOfWeekMAD` + `MAD` with a voting ensemble — a useful starting point if your signal has both daily and weekly structure.

**Resource note**
The sliding-window strategy keeps TSDB load bounded even at a 4w lookback: the long query fires once per `baseline_refresh_interval` (default `1h`) while the latest sample is still fetched every refresh and stitched onto the cached baseline before scoring. `validate --probe` reports the projected per-day query count for each stratified detector — see [`scaling-stratified.md`](scaling-stratified.md).

**Calibration caveat**
The `auto_select` cycle and the `anomaly_confidence_score` / `anomaly_baseline_stability` gauges were designed for rolling-window detectors and are imperfect for stratified ones — the synthetic-injection patterns partially contaminate same-bucket baselines, and the stability proxy compares bucket medians that legitimately differ on a clean signal. For stratified-only configs, prefer explicit detector selection (`auto_select: false`) and consider `defaults.emit_baseline_stability: false`. Full rationale + a future-fix note in [`scaling-stratified.md`](scaling-stratified.md#calibration-confidence-and-auto-select-caveats).

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

## Cohort — Fleet-Relative Comparison

```yaml
detectors:
  - name: Cohort
    params:
      cohort_label: instance    # default; the label that varies between members
      min_cohort_size: 5        # default; cohorts smaller than this are skipped
```

**Best for**  
Fleets of nominally identical workloads — nodes, pods, replicas of a service — where the anomaly only appears when a member is compared against its peers. Each individual series can look perfectly normal in its own rolling window; the divergence is the signal.

**How it works**  
The runner groups every series returned by the query into **cohorts** — series sharing every label except `cohort_label`. For each cohort with at least `min_cohort_size` members:

1. At every timestamp `t` in the rolling window, compute the cross-member median and MAD of the values.
2. Summarise to a single `(cohort_median, cohort_mad)` by taking the median over time of each per-step statistic (which keeps the baseline insensitive to short bursts in one member).
3. For each member, score `|rolling_mean(member) - cohort_median| / cohort_mad` — the member's typical level expressed in cohort-MAD units.

Cohorts below `min_cohort_size` are skipped silently: members receive a zero-score row but no firing. The detector emits an opt-in `cohort_label="<name>"` output label so dashboards can pivot on the chosen grouping.

**Parameters**
- `cohort_label` (default `"instance"`) — the label that varies between cohort members. Common choices: `instance`, `pod`, `replica`. Every *other* label on the series identifies the cohort itself.
- `min_cohort_size` (default `5`) — minimum members required for scoring. Guards against the degenerate two-member case where one member is the cohort's own median by definition.

**Trade-offs**
- Catches the "one bad node in the fleet" case that no single-series detector can.
- **Not** multivariate / covariance-shift analysis — that lives in the roadmap's future-considerations list, not in Cohort itself. Cohort comparison is the restricted, well-understood version of the problem.
- Calibration / `auto_select` are designed around single-series detectors. The runner **skips the calibration cycle entirely for cohort-aware detectors**, so `anomaly_confidence_score` and `anomaly_baseline_stability` are simply not emitted for Cohort. Pair Cohort with explicit detector selection rather than `auto_select`, and prefer a dedicated group when mixing it with single-series detectors so the calibration gauges for the latter aren't blocked by Cohort's presence. The runner emits a one-time boot warning (`cohort_with_auto_select_true`) when it detects a Cohort plan in a group with `auto_select: true`.
- `/debug/inspect` and the `inspect` CLI build the same per-cohort baseline the runner uses, so inspecting one member shows the real cross-member score — not the zero a naive single-series inspection would return.

**Edge cases worth knowing**

- **Constant fleet (cohort MAD = 0).** When most cohort members sit on the same exact value, the median absolute deviation collapses to zero and the score collapses to zero too. This matches the MAD detector's behaviour on a flatline window and biases toward false negatives, not false positives. Cohort is built for **noisy continuous signals** (CPU, latency, throughput). For discrete or quantised signals (`up{}`, integer counters with little variation), pair Cohort with PromQL preprocessing (e.g. `rate()`, `irate()`) that introduces meaningful variation, or use a different detector on a derived signal.
- **Self-inclusion in the baseline.** The cross-cohort median and MAD are computed over **every** member, including the one being scored. For typical fleet sizes (≥ 10) this bias is negligible; for the minimum-size case (`min_cohort_size: 5`) a single outlier nudges the baseline toward itself and slightly shrinks its own score. The current implementation accepts this for simplicity — leave-one-out would require recomputing N baselines per query and the operational gain is small at the fleet sizes Cohort targets.
- **Cohort + `discover:`.** When `discover.variable` and `cohort_label` reference the **same** label, each discover-rendered query receives only one member per cohort — every cohort falls below `min_cohort_size` and Cohort emits zero-score rows for all members. Either pick a `cohort_label` that's *not* one of the discovered variables, or drop the `discover:` block and let the cohort form from a single PromQL fetch.

**Worked example — `cohort_label: instance` across six nodes**

Given six series sharing `job=node` and differing only on `instance`, where five hover at ~10 and one drifts to ~30:
- Cohort median converges to ~10, cohort MAD to ~0.1.
- Score for the divergent member is `|30 - 10| / 0.1 = 200` — well above the default alert threshold of 3.
- Scores for the five clean members are near zero.

See [`patterns.md`](patterns.md) for composing Cohort output with other detectors via PromQL joins on `(id, group, instance)`.

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

## Third-party Detectors via Entry Points

The detector registry is purely entry-point driven: built-ins and out-of-tree wheels register identically through the `promanomaly.detectors` entry point group. Anything `pip install`-able into the runtime image — a private wheel, a community detector like `promanomaly-prophet`, an experimental fork — shows up automatically on `/debug/detectors` and `promanomaly detectors list` without forking the core repo.

```toml
# pyproject.toml of a hypothetical third-party detector package
[project.entry-points."promanomaly.detectors"]
ProphetResidual = "promanomaly_prophet.detector:ProphetResidual"
```

A few constraints the registry enforces:

- **Unique names.** Two packages claiming the same detector `name:` raise `DetectorRegistrationError` at boot — silent overwrite of a built-in by a third-party wheel is not allowed.
- **Required `description:`.** Surfaces in `/debug/detectors`; the registry rejects empty strings.
- **Stable parameter contract.** `defaults` (dict) and `param_spec` (tuple of `ParamSpec`) must agree on names and defaults. Parameter defaults are part of the public API — changing one is a breaking change with a deprecation period.

Unknown detector names raise `UnknownDetectorError` carrying the available list, so misspelled config references produce an actionable error in the runner log instead of a bare `KeyError`.