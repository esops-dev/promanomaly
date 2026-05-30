# Composition Patterns Cookbook

Most advanced use cases are solved by combining promanomaly’s plain Prometheus metrics (`anomaly_score`, `anomaly_outside_threshold`, etc.) with standard PromQL, recording rules, or Alertmanager. No new detector features required.

This page is the **composition cookbook** for everything that looks
like a missing feature but is actually a recurring pattern. If you
catch yourself wishing for a new config knob or a new detector type,
check here first — there's a good chance the recipe already exists.

The features deliberately kept out of promanomaly core (and routed to
composition instead) are documented at the end under
[**Why each of these lives here, not in the detector**](#why-each-of-these-lives-here-not-in-the-detector).

## Rate-of-Change & Acceleration

Detect anomalies in *how fast* a metric changes, not its absolute value.

```yaml
queries:
  - id: queue_growth
    promql: rate(queue_depth_total[5m])
    detectors:
      - name: MAD

  - id: queue_acceleration
    promql: deriv(rate(queue_depth_total[5m])[15m:1m])
    detectors:
      - name: Hampel
```

Detectors work on any numeric series — rates and derivatives are handled identically to raw values.

## Group-Level Rollups

One alert per group instead of per series.

```yaml
# recording rules
- record: anomaly_outside_threshold:group_any
  expr: max by (group) (anomaly_outside_threshold)

- record: anomaly_outside_threshold:group_count
  expr: count by (group) (anomaly_outside_threshold == 1)

- record: anomaly_score:group_max
  expr: max by (group) (anomaly_score)
```

Example alert:
```yaml
- alert: AnomalyGroupBurst
  expr: anomaly_outside_threshold:group_count > 10
  for: 5m
```

## Scaffolding Rules From Your Config

Rather than hand-copying `id`/`group` label sets into alert rules, generate
a starting-point `PrometheusRule` straight from your config:

```bash
promanomaly generate-rules --config config.yaml --output rules.yaml
```

It emits one outside-threshold alert per group/query with the selectors
pre-filled, plus the global `max`-ensemble recording-rule fallback
(`anomaly_outside_threshold_any = max by (id, group) (anomaly_outside_threshold)`)
— the recording-rule equivalent of an `ensemble: { method: max }` block, for
operators who don't use the built-in `ensemble:`. Severities, `for:` windows,
and runbook links are left as `# TODO` placeholders (the project ships no
opinionated thresholds). The generated file is `promtool check rules` clean.
See [cli.md](cli.md#generate-rules).

## Flap Suppression

Use the standard alert `for:` clause:

```yaml
- alert: AnomalyOutsideThreshold
  expr: anomaly_outside_threshold == 1
  for: 5m
```

For duration-based alerts use the built-in `anomaly_duration_seconds` metric.

## Multi-Window Detection

Catch both short spikes and longer trends with the same detector:

```yaml
detectors:
  - name: MAD
    instance: short
    params:
      window: 5m
  - name: MAD
    instance: long
    params:
      window: 1h
```

The `detector_instance` label separates the outputs.

## Absence Detection

For metrics that *should* exist:

```yaml
queries:
  - id: heartbeat
    promql: up{job="critical"}
    expect: true
    expect_grace_runs: 3
    detectors:
      - name: MAD
```

This emits `anomaly_signal_missing` when the series disappears.

For unwatched metrics use `absent_over_time()` directly in alerts.

### Discovery-tracked absence

For dynamic workloads (pods, instances, jobs that come and go) you usually don't know which series should exist ahead of time — write the query once with `discover:` and the detector tracks the set automatically. When a previously-seen series stops reporting for `expect_grace_runs` consecutive runs, `anomaly_signal_missing{id, group, <discovered_labels>}=1` is emitted with the missing series' labels.

```yaml
queries:
  - id: cpu_busy_{{ instance }}
    promql: 'avg by (instance) (rate(node_cpu_seconds_total{instance="{{ instance }}"}[1m]))'
    expect_grace_runs: 3
    discover:
      - variable: instance
        probe: 'up{job="node"}'
        label: instance
    detectors:
      - name: MAD
```

The same `AnomalySignalMissing` reference alert covers both `expect:` (static) and `discover:` (dynamic). See [`operations.md`](operations.md#dynamic-series-discovery) for the full discovery model — Cartesian expansion, probe failure handling, and the `validate --probe` discovery-expansion simulation.

## Per-Group Thresholds

Override the default only where needed:

```yaml
defaults:
  alert_thresholds:
    score: 3.0

groups:
  - name: critical
    queries:
      - id: checkout
        promql: ...
        alert_thresholds:
          score: 2.5
        detectors:
          - name: MAD
```

## Top-N Anomalies Panel (Grafana)

```promql
topk(10, max by (id, group, instance) (anomaly_score))
```

Add `anomaly_duration_seconds` as a second column to distinguish brief spikes from sustained problems.

## Change-Points Composed With Outlier Alerts

Layer change-point detection on top of an ordinary outlier detector so the alert text can distinguish "still anomalous" from "regime just shifted":

```yaml
queries:
  - id: error_rate
    promql: sum by (service) (rate(http_requests_total{status=~"5.."}[5m]))
    detectors:
      - name: MAD                 # ordinary outlier alerts
      - name: BOCPD               # regime-shift detection
      - name: CUSUM               # classical change-point counter
```

Alert recipes:

```promql
# Regime shift happened recently AND the latest sample is still off-baseline.
increase(anomaly_change_point_total{detector="BOCPD"}[10m]) > 0
  and on (id, group) anomaly_outside_threshold{detector="BOCPD"} == 1
```

```promql
# Either change-point detector says "fresh fire in the last minute".
sum by (id, group) (
  rate(anomaly_change_point_total{detector=~"BOCPD|CUSUM"}[1m])
) > 0
```

Do NOT silence change-point alerts during deploys inside the detector — fire on the change-point in Prometheus, silence in Alertmanager during the deploy window via webhooks. Damping scores during deploys hides the failure modes operators most need to see.

## Cross-Tool: promforecast + promanomaly

```promql
(forecast_deviation_outside_band == 1)
  and on (id, group)
(anomaly_outside_threshold == 1)
```

Both tools follow the same `id` + `group` label contract — see
[`when-to-use.md`](when-to-use.md) for the decision tree and
[`LABELS_CONTRACT.md`](../LABELS_CONTRACT.md) for the exact label
guarantee. Either direction of the join is meaningful: requiring
both means "slow capacity drift AND a fast-moving anomaly on the same
signal right now"; an `or` rollup means "anything wrong with this
signal".

A useful production pattern is one recording rule for each:

```yaml
- record: signal_in_trouble:any
  expr: |
    forecast_deviation_outside_band == 1
      or on (id, group) anomaly_outside_threshold == 1

- record: signal_in_trouble:both
  expr: |
    forecast_deviation_outside_band == 1
      and on (id, group) anomaly_outside_threshold == 1
```

…and an Alertmanager `inhibition_rule` so an `:both` firing
suppresses the noisier `:any`.

## Deploy-Time Silencing

Anomaly scores should never be damped, masked, or paused during
deploys, rolling restarts, or maintenance windows — real outages
happen during those windows precisely because deploys cause them.
Silencing belongs in Alertmanager, fired by a webhook from your
deploy pipeline:

```bash
# In your CD pipeline, before the rollout:
curl -X POST "$AM/api/v2/silences" -d '{
  "matchers": [
    {"name": "id", "value": "checkout_p95", "isRegex": false}
  ],
  "startsAt":  "'"$(date -u +%FT%TZ)"'",
  "endsAt":    "'"$(date -u -d '+30 min' +%FT%TZ)"'",
  "createdBy": "ci-bot",
  "comment":   "checkout deploy $CI_PIPELINE_URL"
}'
```

The detector keeps emitting `anomaly_score` and
`anomaly_outside_threshold` exactly as before; Alertmanager just
silently drops the resulting alert for the silence's lifetime. The
signal stays visible on Grafana for post-incident analysis without
firing a page. **This is the correct way to do it** — see
[**Why each of these lives here, not in the detector**](#why-each-of-these-lives-here-not-in-the-detector)
for the rationale.

## Confidence-Gated Alerts

When auto-select or stratified detectors are emitting
`anomaly_confidence_score` and `anomaly_baseline_stability`, gate
alerts on both signals so warm-up windows and calibration noise
don't page:

```promql
anomaly_outside_threshold == 1
  and on (id, group, detector) anomaly_confidence_score      > 0.7
  and on (id, group, detector) anomaly_baseline_stability    > 0.6
```

The bundled `AnomalyConfidenceLow` PrometheusRule does the inverse
side of the same gate (alert on low confidence on a firing detector
so you find out the calibration is degrading before you stop
trusting the score). See [`confidence.md`](confidence.md) for the
underlying formulas.

## Practical-Significance Floors

Statistical significance ≠ operational significance. A 3-MAD move on
a counter that went from `0.01` errors/s to `0.012` errors/s is
real, but probably not page-worthy. Set a floor:

```yaml
defaults:
  min_abs_delta:      0.05    # 0.05 errors/s minimum before
                              # anomaly_outside_threshold can flip
  min_relative_delta: 0.05    # OR 5% of baseline
```

`anomaly_score` is unaffected — dashboards still see the raw signal.
Only the `anomaly_outside_threshold` bit is gated. Tune per-query
where the group-level floor is wrong:

```yaml
queries:
  - id: chatty_signal
    promql: ...
    min_abs_delta: 0.5        # noisier signal needs a higher floor
    detectors:
      - name: MAD
```

## Why each of these lives here, not in the detector

A recurring pattern in design review is "this should be a feature
flag in the detector". Most of the time, the answer is that PromQL,
recording rules, and Alertmanager already cover it — and growing the
detector to cover it duplicates ecosystem tooling poorly. The
features kept out of core, with the composition recipe that replaces
them:

| Asked for | Lives instead in | Recipe |
|---|---|---|
| Rate-of-change / acceleration detector | PromQL `rate()` / `deriv()` wrapper on the source query | [Rate-of-Change & Acceleration](#rate-of-change--acceleration) |
| Group / fleet rollup metrics | Recording rules over `anomaly_*` | [Group-Level Rollups](#group-level-rollups) |
| `for:`-style hysteresis inside the detector | Alertmanager `for:` on the firing rule | [Flap Suppression](#flap-suppression) |
| Multi-window detection as a config knob | Repeat the detector with `instance:` | [Multi-Window Detection](#multi-window-detection) |
| Per-detector silencing windows / deploy-aware damping | Alertmanager silences, fired by webhook | [Deploy-Time Silencing](#deploy-time-silencing) |
| Weighted / Bayesian / ML ensemble fusion | Compose `anomaly_outside_threshold` via recording rules | [Group-Level Rollups](#group-level-rollups) |
| "Known-noisy series" mute lists | Raise the threshold per query, swap detector, or drop the series from config | (no recipe — these are tuning, not silencing) |
| Built-in alerting policy (severity escalations, "page only on second occurrence") | Alertmanager routes + grouping | (Alertmanager docs) |
| Per-query post-processing PromQL | Recording rules on the emitted anomaly metrics | (Prometheus recording-rule docs) |
| Built-in UI | Grafana with the bundled dashboards | [`dashboards/grafana/`](../dashboards/grafana/) |

The detector's job is **emitting the signal**. Anything that looks
like alerting policy, silencing, severity, or rollup belongs in the
surrounding ecosystem. Pushing those features back into composition
recipes is how promanomaly stays small, stateless, and
ecosystem-friendly.
