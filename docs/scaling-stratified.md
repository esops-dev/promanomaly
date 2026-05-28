# Scaling Stratified Detectors

Stratified detectors (`HourOfDayMAD`, `DayOfWeekMAD`, etc.) compare the current sample against historical values from the **same time bucket** (same hour-of-day or same day-of-week). This needs multi-week lookback data, which would be expensive if fetched on every refresh.

promanomaly uses a smart two-tier fetch strategy to keep TSDB load low while keeping scores fresh.

## Fetch Strategy

1. **Short fetch** — runs every `refresh_interval` (typically 15s–1m). Brings in the latest small window.
2. **Baseline fetch** — runs every `baseline_refresh_interval` (default `1h`). Pulls the full historical window and is cached per `(group, query)`.
3. **Stitch** — fresh samples replace any overlap in the cached baseline. The combined frame is passed to the detector.

Non-stratified detectors in the same query are unaffected — they still see only their normal rolling window.

**Result**: heavy baseline queries drop from ~1,440/day to ~24/day per stratified query (60× reduction).

**Why one big query?**  
A single range query over the full baseline window is cheaper (fewer HTTP round-trips and query-planning overhead) than many per-bucket queries.

## Tuning `baseline_refresh_interval`

```yaml
defaults:
  baseline_refresh_interval: 1h   # change here or per-group
```

| Workload Type                  | Recommended Interval |
|--------------------------------|----------------------|
| Typical diurnal / weekly patterns | `1h` (default)     |
| Fast-changing buckets          | `15m` – `30m`        |
| Very stable seasonality        | `2h` – `4h`          |
| TSDB under heavy load          | `6h`+                |

The **latest sample** is always fresh. Only the historical baseline updates at this cadence.

## Verify Cost Before Rollout

Run:

```bash
promanomaly validate --config config.yaml --probe
```

It shows `baseline_queries_per_day` and other cost projections for every stratified detector. Use `--strict` in CI to block expensive configs before they merge.

## Caching & HA Behavior

- Cache key is `(group, query)`. Lives in memory.
- Automatically cleared on restart or when window/step changes.
- Reloads that change the template PromQL, rename / remove the query, or drop the stratified detector entirely also evict the cache entry — no stale stitching across config changes.
- In HA setups only the **leader** runs baseline fetches. On failover the new leader does a one-time full baseline fetch for all stratified detectors (brief TSDB spike). Followers don't log stratified-config warnings on boot (the leader does).

## Boot-Time Warnings

The runner logs two structured warnings on boot and reload — grep them out of container logs after rolling out a stratified config:

- `stratified_baseline_refresh_invalid` — `baseline_refresh_interval` resolves to `<= 0`. The sliding-window strategy is silently disabled for that detector (per-run full-window fetch).
- `stratified_baseline_refresh_below_refresh_interval` — value is positive but shorter than `server.refresh_interval`. The cache is always stale, so the heavy long-history query fires every refresh anyway — defeats the strategy without crashing.

## Multiple Instances of One Stratified Detector

You can configure the same stratified detector under one query with different `instance:` labels (the v0.2 multi-window pattern). When you do:

- The cache stores **one** baseline window per query, sized to `max(lookback)` across the instances.
- Refresh fires at `min(baseline_refresh_interval)` across instances — fresher data for everyone, more TSDB load.
- Each instance's `_slice_for_detector` trims the cached frame back to its own lookback before scoring, so the per-instance verdicts remain independent.

Practical implication: a `lookback: 2w` instance sharing a query with a `lookback: 4w` instance pays the 4w fetch cost; the 2w-instance just scores on the trailing 2w of the cached frame.

## Important Caveats

**Auto-select, confidence, and baseline stability work less reliably** on stratified detectors:

- `anomaly_baseline_stability` often scores low even on clean seasonal data (different buckets have different natural medians).
- Synthetic injection patterns contaminate the exact buckets the detector uses → F1 scores are biased downward.
- `auto_select` usually favors rolling-window detectors.

**Recommendation**:  
For groups that are mostly or entirely stratified, set `auto_select: false` and choose detectors explicitly. Also consider `defaults.emit_baseline_stability: false`.

## Backtesting Stratified Detectors

`make backtest GROUP=<g>` and `make backtest-inject GROUP=<g>` honour the detector's preferred window automatically — a stratified detector with `lookback: 4w` triggers a 4-week TSDB fetch even when the group's `defaults.window` is 1h. Without this the bucket-median baseline would collapse and reported precision / recall would be meaningless.

**TSDB-retention note.** In `--inject` mode without an explicit `--from`, backtest fetches `2 × lookback` for stratified detectors (so it has two distinct latest-sample positions for the sliding-window evaluation). For a `DayOfWeekMAD` at the 4w default this asks the TSDB for 8 weeks of data — make sure your retention covers that, or pass `--from -<duration>` to bound the fetch. Non-stratified detectors keep the previous `4 × defaults.window` multiplier (typically a few hours).

## Example Configuration

A production-shaped reference config combining `HourOfDayMAD` + `DayOfWeekMAD` + `MAD` with a voting ensemble lives at [`examples/configs/weekly-baseline.yaml`](../examples/configs/weekly-baseline.yaml). Drop it next to your other group configs as a starting point.

## Choosing the Right Bucket Dimension

Not sure whether hourly or daily buckets fit your signal? Run:

```bash
promanomaly calibrate-buckets --query 'your_promql_here' --from -7d
```

It runs autocorrelation analysis and prints a ready-to-paste detector config snippet.

See the [CLI Reference](cli.md) for full options.