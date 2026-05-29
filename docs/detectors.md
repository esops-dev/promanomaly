# Detectors

Every detector is a stateless scorer. It looks at a rolling window of recent samples for one time series and returns an `anomaly_score` for the newest sample plus a `baseline` value you can overlay on dashboards.

Sensitivity is controlled globally with `alert_thresholds.score` (default `3.0`). No per-detector tuning knobs.

For “which detector should I use?”, see [`decision-tree.md`](decision-tree.md).
For "promforecast vs. promanomaly?", see [`when-to-use.md`](when-to-use.md).

## How each section is structured

Every detector page below follows the same five-section template so you
can scan across detectors without re-reading prose for each one:

1. **What it catches** — the anomaly shapes the detector is designed for.
2. **What it misses** — failure modes the detector is structurally blind
   to. Reach for a different detector or compose via PromQL.
3. **How to tune** — the parameters that matter, with sensible default
   reasoning. Defaults are part of the public API; changing them is a
   breaking change with a deprecation window (see
   [`config-schema.md`](config-schema.md)).
4. **When to choose it** — the signal shapes where it's the right pick.
5. **When *not* to choose it** — signal shapes where it's actively the
   wrong pick (not just suboptimal).

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

**What it catches.** Single-point and small-burst outliers on noisy,
heavy-tailed, or non-Gaussian signals — p99 latency, error rates,
queue depth, throughput. Robust by construction: one bad point cannot
pull the baseline, so the score on a clean window stays low and the
score on a spike stays high.

**What it misses.**
- Slowly drifting baselines inside the rolling window — the median
  smears the drift and the latest sample stops scoring as anomalous.
  Reach for `Hampel`.
- Step changes that have already filled most of the window. By the
  time the median has shifted, the change is "the new normal" — reach
  for `BOCPD` or `CUSUM`.
- Clustered multi-point anomalies that fill several adjacent samples
  poison the median + MAD itself. Enable `recursive_trim` to recover.

**How to tune.**
- `recursive_trim` (default `false`) — when true, run the baseline
  once, drop in-window points above `trim_threshold`, and recompute
  the median + MAD once more before scoring the latest sample.
  Bounded to a single trim pass for predictability. Mitigates the
  clustered-anomaly limitation: a burst of 4–5 anomalous points in
  the rolling window normally poisons the very median MAD is
  computed against, and the latest sample looks normal. With
  `recursive_trim` on, the burst is excluded from the recomputed
  baseline and the latest sample scores as the operator would
  expect.
- `trim_threshold` (default `3.0`) — MAD-units cutoff for the trim
  pass. Matches the conventional 3-MAD anomaly threshold and the
  default `alert_thresholds.score`. **Deliberately separate from
  `alert_thresholds.score`** — alerting policy can change without
  silently re-shaping the baseline. Raise it when the rolling
  window legitimately contains points beyond 3 MADs that you don't
  want stripped; lower it for very clean signals where 3 MADs is
  already noise.

  Worked example — `clustered_spikes(cluster=6, spike=10.0)`:
  - Without trim: baseline pulled toward the spike value, score ≈ 0.
  - With trim: baseline snaps back to the pre-burst mean, score well above 3.

Window size is set globally via `defaults.window`.

**When to choose it.** The default for almost every noisy signal. If
you don't have a strong reason to pick something else, pick this.

**When *not* to choose it.** Signals where the baseline genuinely
drifts inside the window (use `Hampel`), where the regime steps and
you want fast detection (use `BOCPD` or `CUSUM`), or where the
canonical anomaly is "one member of a fleet looks different" (use
`Cohort`).

## Hampel — Local-window Robust Outlier Filter

```yaml
detectors:
  - name: Hampel
    params:
      t0: 3                  # default
      recursive_trim: false  # default
      trim_threshold: 3.0    # default
```

**What it catches.** Same signal shapes as MAD — heavy-tailed,
non-Gaussian, noisy — but on signals where the baseline drifts
slowly within the rolling window. Computes median and MAD over a
small local sub-window of `2*t0 + 1` points around the current
sample, so the baseline tracks drift natively instead of being
dragged by the full window.

**What it misses.**
- Step changes — the local sub-window picks up the new regime
  immediately, so the latest sample after a step looks normal.
  Reach for `BOCPD` or `CUSUM` for step detection.
- Clustered multi-point anomalies inside the local sub-window
  (small by design) poison the local median + MAD. Enable
  `recursive_trim` to recover.
- Signals where the baseline is genuinely flat — `MAD` is cheaper
  and equivalent.

**How to tune.**
- `t0` (default `3`) — half-width of the local sub-window. Larger
  values behave more like classical MAD; smaller values track drift
  faster.
- `recursive_trim` (default `false`) — when true, recompute the
  local median + MAD once after excluding sub-window points above
  `trim_threshold` from the first pass. Same single-iteration bound
  as MAD. Particularly useful for Hampel because the local
  sub-window is small: a 4-point cluster right at the tail of the
  window otherwise fills most of it and the latest sample looks
  normal.
- `trim_threshold` (default `3.0`) — local-MAD-units cutoff for the
  trim pass; same default and decoupling rationale as MAD's
  `trim_threshold`.

**When to choose it.** Noisy signals where the baseline is visibly
moving inside your rolling `window` (slow regression, slowly growing
capacity, business-hours ramp) and you want point-level outlier
detection that doesn't fire constantly because the rolling MAD is
chasing the drift.

**When *not* to choose it.** Stable baselines (use `MAD`), or
step-shifting regimes (use `BOCPD` / `CUSUM`).

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

**What it catches.** Anomalies on roughly Gaussian signals with a
stable but slowly drifting mean: cache hit rate, sustained
throughput, slowly growing capacity. Maintains an EWMA mean and
EWMA variance over the rolling window; the score is the classic
z-score `|y - ewma_mean| / sqrt(ewma_variance)`. The EWMA tracks
slow drift natively, so a regression that creeps in over hours does
not pollute the baseline the way a fixed-window mean would.

**What it misses.**
- Heavy-tailed signals. On p99 latency or request rates with fat
  tails, the variance estimator inflates after the first big spike,
  the z-score collapses, and you stop catching subsequent anomalies.
  `MAD` and `Hampel` are the right tools for those.
- Sparse intermittent signals — the variance estimator goes near-
  zero between events and the next event scores extreme even when
  it's normal for that signal.

**How to tune.**
- `alpha` (default `0.05`) — EWMA smoothing factor. Smaller values
  track the mean more slowly; larger values adapt faster but let
  recent anomalies pollute the baseline more.

**When to choose it.** Smooth, roughly-Gaussian, low-noise signals
where drift is the dominant baseline behaviour — cache hit rate
inching down over the day, capacity slowly growing as you onboard
customers, success rate that's almost flat but not quite.

**When *not* to choose it.** Anything heavy-tailed (p99 latency,
error rates), bursty traffic (request rates with diurnal pattern),
or sparse counters (rare events). Reach for `MAD` or `IQR`.

## IQR — Tukey Interquartile Range

```yaml
detectors:
  - name: IQR
    params:
      k: 1.5           # default
```

**What it catches.** Non-parametric outliers with a sensitivity
profile that differs from MAD — lighter on the tails, more weight
on the middle 50% of the window. Computes `Q1` and `Q3` and flags
samples outside `[Q1 - k*IQR, Q3 + k*IQR]`; the score is the
normalised distance outside the band, in IQR units.

**What it misses.**
- Slow baseline drift (use `Hampel` or `ZScoreEWMA`).
- Clustered multi-point anomalies that shift `Q1` or `Q3` — IQR has
  no `recursive_trim` equivalent, so a sustained burst can pull the
  quartiles enough to silence the latest sample.

**How to tune.**
- `k` (default `1.5`) — multiplier for the IQR band. `1.5` is
  Tukey's classical "outlier" definition; `3.0` is "far outlier".

**When to choose it.** As the second detector in a `voting` ensemble
with `MAD`. The two disagree in interesting ways on heavy-tailed
signals, and `voting: min_detectors=2` cuts the false-positive rate
of either detector alone without losing real anomalies.

**When *not* to choose it.** As the only detector on a heavy-tailed
signal — `MAD` is the better default. As a primary detector for
drift (use `Hampel` / `ZScoreEWMA`).

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

**What it catches.** Anomalies on signals with strong diurnal
patterns where a fixed-window MAD smears the daily shape and either
false-positives every off-peak hour or false-negatives genuine
anomalies. The canonical case: "Tuesday 3am looks unlike prior 3am
samples." Buckets every sample in the lookback window by its UTC
hour-of-day (24 buckets by default), then scores the latest sample
against the same-bucket median and MAD. The runner automatically
expands the per-query fetch window, so it composes with short-window
detectors (`MAD` / `Hampel`) in the same query without forcing the
group's `defaults.window` to grow.

**This is still baseline detection, not forecasting.**
The detector scores the present against historical observations
from the same hour-of-day bucket. It never predicts future values —
that's promforecast's job.

**What it misses.**
- Weekly periodicity. If weekday vs. weekend behaviour differs
  materially, the per-hour median collapses across both and you'll
  either over-fire on weekends or miss weekday anomalies. Use
  `DayOfWeekMAD` instead.
- Sub-hour fluctuations inside a bucket — by design, the bucket
  width is the granularity.

**How to tune.**
- `lookback` (default `7d`) — how far back to fetch baseline
  samples. Sized to give every bucket ≥ 7 observations to median
  over.
- `bucket_width` (default `1h`) — must divide 24h evenly (e.g.
  `30m`, `1h`, `2h`, `4h`). Smaller buckets sharpen the diurnal
  shape but each bucket has fewer observations.

**Time zone caveat.** Buckets are computed against **UTC** wall-clock
time because Prometheus stores UTC. If your operations team thinks
in a non-UTC timezone (e.g. JST = UTC+9), "9 AM local" buckets line
up with UTC midnight bucket; bucket boundaries don't align with
local "hour of day". This is fine for periodic anomaly detection but
can confuse on-call ("the alert says hour 0 was anomalous; it's 9 AM
here"). A local-timezone option is deferred until enough operators
ask for it — for now, document the offset alongside your runbook or
use a `bucket_width` that's a divisor of your timezone offset to
keep buckets aligned.

**Resource note.** A long lookback against a large query can pull a
lot of data; respect `safety.max_series_per_query` and
`safety.max_total_series`. The full day-of-week / multi-week version
is `DayOfWeekMAD` (below) and uses the runner's sliding-window fetch
strategy to keep TSDB load bounded.

**When to choose it.** Diurnal signals whose daily shape is
consistent across the week — request rates, batch counters, business
KPIs where Monday's 3am ≈ Tuesday's 3am ≈ Sunday's 3am.

**When *not* to choose it.** Weekly-shaped signals (use
`DayOfWeekMAD`), signals without strong periodicity (use plain
`MAD`), or signals whose daily shape itself changes (use `BOCPD`).

## DayOfWeekMAD — Stratified by (Day-of-Week, Hour-of-Day)

```yaml
detectors:
  - name: DayOfWeekMAD
    params:
      lookback: 4w                    # default
      bucket_width: 1h                # default
      baseline_refresh_interval: 1h   # default
```

**What it catches.** Anomalies on weekly-periodic signals where
weekend traffic behaves nothing like weekday traffic — B2B request
rates that go quiet on Saturdays, batch jobs that only run on
Mondays, business-hours error rates that differ from overnight.
Buckets every sample in the lookback window by
`(weekday(), hour-of-day)` (default 7x24 grid, all in UTC) and
scores the latest sample against the same-bucket median + MAD. The
`weekday()` value follows Python's `datetime.weekday()` convention
(Monday=0).

**This is still baseline detection, not forecasting.**
Same carve-out as `HourOfDayMAD`. The detector scores the present
against historical observations from the same `(day-of-week,
hour-of-day)` bucket — it never predicts future values.

**What it misses.**
- Anomalies that span a full bucket — if Sunday afternoons are
  always slightly off, the median already encodes "slightly off"
  and a slightly-more-off Sunday won't score.
- Recent regime shifts (last week was a new product launch and
  this week's baseline is the new normal) — the multi-week median
  is dragging old behaviour. Pair with `BOCPD` for that case.
- Sub-bucket fluctuations — by design.

**How to tune.**
- `lookback` (default `4w`) — multi-week window. Sized in weeks so
  every bucket has several observations to median over. Accepts `w`
  (weeks), `d` (days), `h` (hours), `m` (minutes).
- `bucket_width` (default `1h`) — within-day bucket width; must
  divide 24h evenly.
- `baseline_refresh_interval` (default `1h`) — opts the detector
  into the runner's sliding-window fetch strategy: the heavy
  multi-week query runs at this cadence rather than every
  `server.refresh_interval`. See
  [`scaling-stratified.md`](scaling-stratified.md) for the operational
  rationale.

**Time zone caveat.** Both `weekday()` and the within-day bucket are
computed in **UTC**. For operators in non-UTC timezones the weekday
boundary lands at local times other than midnight: in JST (UTC+9),
the UTC "Sunday" bucket covers Sunday 09:00 → Monday 09:00 local.
That's fine for weekly periodicity detection but operators reading
dashboards should remember that "weekday 5 (Saturday)" in a metric
label refers to UTC Saturday, not local Saturday. Same
deferred-timezone-option rationale as `HourOfDayMAD`.

**Resource note.** The sliding-window strategy keeps TSDB load
bounded even at a 4w lookback: the long query fires once per
`baseline_refresh_interval` (default `1h`) while the latest sample
is still fetched every refresh and stitched onto the cached baseline
before scoring. `validate --probe` reports the projected per-day
query count for each stratified detector — see
[`scaling-stratified.md`](scaling-stratified.md).

**Calibration caveat.** The `auto_select` cycle and the
`anomaly_confidence_score` / `anomaly_baseline_stability` gauges
were designed for rolling-window detectors and are imperfect for
stratified ones — the synthetic-injection patterns partially
contaminate same-bucket baselines, and the stability proxy compares
bucket medians that legitimately differ on a clean signal. For
stratified-only configs, prefer explicit detector selection
(`auto_select: false`) and consider
`defaults.emit_baseline_stability: false`. Full rationale + a
future-fix note in
[`scaling-stratified.md`](scaling-stratified.md#calibration-confidence-and-auto-select-caveats).

**When to choose it.** Weekly-periodic signals where weekday and
weekend look genuinely different — B2B traffic, business KPIs,
batch-style workloads. The combined daily + weekly stratification
keeps the baseline tight where `HourOfDayMAD` would smear.

**When *not* to choose it.** Signals whose daily shape is the same
across the week (use `HourOfDayMAD` — cheaper and a 1w lookback is
enough), signals without seasonality (use plain `MAD`), or signals
whose seasonal shape itself shifts frequently (use `BOCPD`).

**Example.**
[`examples/configs/weekly-baseline.yaml`](../examples/configs/weekly-baseline.yaml)
ships a production-shaped reference combining `HourOfDayMAD` +
`DayOfWeekMAD` + `MAD` with a voting ensemble — a useful starting
point if your signal has both daily and weekly structure.

## BOCPD — Bayesian Online Change-Point Detection

```yaml
detectors:
  - name: BOCPD
    params:
      hazard: 250        # default
      threshold: 0.5     # default
      lag: 15            # default
```

**What it catches.** Regime shifts on signals where you want a
probabilistic change-point score — deploys, rolling restarts,
autoscaler steps, anything that breaks the baseline rather than
spiking through it. Models the observation stream with a
Normal-Gamma conjugate prior and maintains a posterior distribution
over the *run length* (samples since the last change-point). The
score is the posterior mass at recent run lengths
(`r_t <= lag`) — high when a change-point happened in the last
`lag` samples. Fires when score ≥ `threshold`; `threshold` is a
per-detector override of `alert_thresholds.score` because the score
is a probability, not a sigma multiplier.

**What it misses.**
- Point spikes — BOCPD scores low on isolated outliers that don't
  shift the regime. Pair with `MAD` for that.
- Strongly non-Gaussian distributions. The Normal-Gamma prior
  assumes Gaussian-ish observations; heavy-tailed signals produce
  noisy change-point probabilities.
- Periodic anomalies that look like regime shifts to BOCPD — the
  posterior keeps re-firing every period.

**How to tune.**
- `hazard` (default `250`) — expected interval between change-points
  in samples. Raise on signals where breaks are rare (lower
  false-positive rate but slower detection); lower for chatty
  environments.
- `threshold` (default `0.5`) — posterior probability above which
  the detector fires. The probability scale means thresholds above
  `0.7` are very confident; below `0.3` is essentially random.
- `lag` (default `15`) — inclusive run-length window counted as
  "recent". Larger values catch slower transitions; smaller values
  fire only on abrupt shifts.

**Trade-offs.** Probabilistic and confidence-gateable; more
expensive than CUSUM (`O(window²)` per fit, still cheap in absolute
terms); assumes Gaussian-ish observations.

**When to choose it.** Step-change detection where you want a
confidence-gated alert: "fire when the posterior probability that
the regime just shifted is ≥ 0.7". Useful when you want to
distinguish "still anomalous" from "regime just shifted" via the
composition recipe in [`patterns.md`](patterns.md#change-points-composed-with-outlier-alerts).

**When *not* to choose it.** Point-spike detection (use `MAD` /
`Hampel`), or heavy-tailed signals where the Normal-Gamma prior
mis-fits. For simpler step detection, `CUSUM` is faster.

## CUSUM — Cumulative Sum

```yaml
detectors:
  - name: CUSUM
    params:
      k: 0.5      # default
      h: 5.0      # default
```

**What it catches.** Fast, classical step-change detection. Tracks
running upper/lower cumulative sums of `(x - median - k*sigma)`
against a MAD-derived sigma estimate. A change-point fires on a
*fresh* crossing of `h*sigma` at the latest sample (no
double-counting on overlapping rolling-window calls). The score is
`max(|S+|, |S-|) / sigma`, in sigma units — same scale as MAD/Hampel.

**What it misses.**
- Point spikes — same blind spot as BOCPD; pair with `MAD`.
- Gradual drift that never accumulates enough sigma to cross `h` —
  reach for `Hampel` or `ZScoreEWMA`.
- Probabilistic interpretation — `CUSUM` returns a sigma multiplier,
  not a probability. Use `BOCPD` when you want
  `anomaly_confidence_score`-style gating.

**How to tune.**
- `k` (default `0.5`) — reference value (in sigma units); slack
  subtracted before accumulation. Higher `k` ignores smaller drifts;
  lower `k` fires on subtler shifts.
- `h` (default `5.0`) — decision threshold (in sigma units). The
  conservative default keeps the false-positive rate low; halve it
  on signals where you want to catch smaller-magnitude shifts.

**Trade-offs.** Single-pass, `O(window)`; intuitive sigma-multiplier
units; less natural for non-Gaussian likelihoods but the
MAD-derived sigma helps on heavy-tailed signals.

**When to choose it.** Fast, time-tested step-change detection for
operators who don't need a probabilistic score. The workhorse of
industrial process control; pairs well with `BOCPD` when you want
both probabilistic and classical signals on the same metric.

**When *not* to choose it.** When you want a probability-scale
output (use `BOCPD`), gradual-drift detection (use `Hampel` /
`ZScoreEWMA`), or point-spike outlier detection (use `MAD`).

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

**What it catches.** Anomalies in fleets of nominally identical
workloads — nodes, pods, replicas of a service — where the anomaly
only appears when a member is compared against its peers. Each
individual series can look perfectly normal in its own rolling
window; the divergence is the signal. The runner groups every series
returned by the query into **cohorts** — series sharing every label
except `cohort_label`. For each cohort with at least
`min_cohort_size` members:

1. At every timestamp `t` in the rolling window, compute the
   cross-member median and MAD of the values.
2. Summarise to a single `(cohort_median, cohort_mad)` by taking the
   median over time of each per-step statistic (which keeps the
   baseline insensitive to short bursts in one member).
3. For each member, score
   `|rolling_mean(member) - cohort_median| / cohort_mad` — the
   member's typical level expressed in cohort-MAD units.

Cohorts below `min_cohort_size` are skipped silently: members
receive a zero-score row but no firing. The detector emits an opt-in
`cohort_label="<name>"` output label so dashboards can pivot on the
chosen grouping.

**What it misses.**
- True multivariate / covariance-shift effects across multiple
  signals — that's a different (and harder) problem; deferred to v2.
- Anomalies where the whole fleet shifts together. By construction,
  Cohort is blind to a regression that affects every member equally.
  Pair Cohort with a single-series detector in the same query to
  cover both shapes.
- Constant fleets (cohort MAD = 0). When most cohort members sit on
  the same exact value, the median absolute deviation collapses to
  zero and the score collapses to zero too. Cohort is built for
  **noisy continuous signals** (CPU, latency, throughput); use
  PromQL preprocessing (`rate()`, `irate()`) on discrete signals.
- The "one is the cohort median by definition" case at very small
  cohort sizes — guard with `min_cohort_size: 5` or higher.

**How to tune.**
- `cohort_label` (default `"instance"`) — the label that varies
  between cohort members. Common choices: `instance`, `pod`,
  `replica`. Every *other* label on the series identifies the cohort
  itself.
- `min_cohort_size` (default `5`) — minimum members required for
  scoring. Guards against the degenerate two-member case where one
  member is the cohort's own median by definition. Raise to 10+ on
  large fleets where self-inclusion bias is a concern.

**Trade-offs.**
- Calibration / `auto_select` are designed around single-series
  detectors. The runner **skips the calibration cycle entirely for
  cohort-aware detectors**, so `anomaly_confidence_score` and
  `anomaly_baseline_stability` are simply not emitted for Cohort.
  Pair Cohort with explicit detector selection rather than
  `auto_select`, and prefer a dedicated group when mixing it with
  single-series detectors so the calibration gauges for the latter
  aren't blocked by Cohort's presence. The runner emits a one-time
  boot warning (`cohort_with_auto_select_true`) when it detects a
  Cohort plan in a group with `auto_select: true`.
- `/debug/inspect` and the `inspect` CLI build the same per-cohort
  baseline the runner uses, so inspecting one member shows the real
  cross-member score — not the zero a naive single-series inspection
  would return.

**Edge cases worth knowing.**
- **Self-inclusion in the baseline.** The cross-cohort median and
  MAD are computed over **every** member, including the one being
  scored. For typical fleet sizes (≥ 10) this bias is negligible;
  for the minimum-size case (`min_cohort_size: 5`) a single outlier
  nudges the baseline toward itself and slightly shrinks its own
  score. The current implementation accepts this for simplicity —
  leave-one-out would require recomputing N baselines per query and
  the operational gain is small at the fleet sizes Cohort targets.
- **Cohort + `discover:`.** When `discover.variable` and
  `cohort_label` reference the **same** label, each discover-rendered
  query receives only one member per cohort — every cohort falls
  below `min_cohort_size` and Cohort emits zero-score rows for all
  members. Either pick a `cohort_label` that's *not* one of the
  discovered variables, or drop the `discover:` block and let the
  cohort form from a single PromQL fetch.

**Worked example — `cohort_label: instance` across six nodes.**
Given six series sharing `job=node` and differing only on `instance`,
where five hover at ~10 and one drifts to ~30:
- Cohort median converges to ~10, cohort MAD to ~0.1.
- Score for the divergent member is `|30 - 10| / 0.1 = 200` — well
  above the default alert threshold of 3.
- Scores for the five clean members are near zero.

**When to choose it.** Identical-workload fleets — node-exporter
targets, kube-state-metrics per-pod data, per-replica request
latency — where "one is misbehaving compared to its peers" is the
canonical anomaly shape. The case no single-series detector can see.

**When *not* to choose it.** Single-series signals (use any of the
single-series detectors), discrete / quantised signals without
PromQL preprocessing, or fleets where the canonical anomaly is the
whole fleet shifting together (pair with a single-series detector
on a `sum`/`avg`-aggregated query, or skip Cohort entirely).

See [`patterns.md`](patterns.md) for composing Cohort output with
other detectors via PromQL joins on `(id, group, instance)`.

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

When `auto_select: true`, the runner periodically injects three canonical synthetic anomalies (point spike, step change, level drift) into each series's rolling window and measures every configured detector's F1. The winner's score samples are emitted and a standalone `anomaly_best_detector{...,detector=<name>} = 1` info gauge names the winner; the losers are suppressed. `auto_select: "explicit_plus_best"` keeps every detector's output and still names the winner via `anomaly_best_detector` — useful while tuning. The marker is a separate series rather than a label on the per-detector metrics, so the winner can flip between cycles without churning the identity of `anomaly_score`/`anomaly_outside_threshold`/`anomaly_change_point_total`.

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