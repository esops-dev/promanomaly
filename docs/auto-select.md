# Auto-Select Best Detector Per Series

Auto-select lets promanomaly automatically choose the best detector for every series. Same UX as promforecast’s `auto_select`.

## When to Use It

- You configured multiple detectors per query and don’t want separate alert rules for each.
- You manage many groups and hand-tuning isn’t realistic.
- You want an objective “this detector performed best on real data” justification.

## When Not to Use It

- Only one detector per query.
- You prefer full explicit control over adaptability.
- TSDB query budget is extremely tight.

## Configuration

```yaml
groups:
  - name: my_group
    auto_select: true                 # false | true | "explicit_plus_best"
    auto_select_interval: 24h         # default
    queries:
      - id: requests
        promql: rate(http_requests_total[1m])
        detectors:
          - name: MAD
          - name: Hampel
          - name: IQR
```

| Option                        | Behaviour |
|-------------------------------|-----------|
| `false` (default)             | Every detector emits its own score |
| `true`                        | Only the winner emits; the winner is named by `anomaly_best_detector` |
| `"explicit_plus_best"`        | All detectors emit + winner named by `anomaly_best_detector` (great for tuning) |

## How It Works

Every `auto_select_interval` (default 24h) the detector runs a quick calibration per series:

1. Scores the real rolling window → counts **false positives**.
2. Injects three synthetic anomalies (point spike, step change, level drift) → counts **true positives** and **false negatives**.
3. Computes precision, recall, and F1 score.

Highest F1 wins. Ties broken by precision, then by order in the config.

## Output

When `auto_select: true`, only the winner’s metrics appear, and a separate
`anomaly_best_detector` info gauge names the winner:

```
anomaly_score{..., detector="MAD", ...} 4.7
anomaly_best_detector{..., detector="MAD", ...} 1
```

`anomaly_best_detector=1` is emitted only for the winner, only when
auto-select is enabled. It is a standalone series rather than a label on
`anomaly_score`/`anomaly_outside_threshold`/`anomaly_change_point_total`
**on purpose**: the winner can change between calibration cycles, and a
label that appears and disappears would churn those series' identity —
breaking `rate()`/`increase()` on the change-point counter and resetting
`for:` timers on `outside_threshold` alerts. Keeping the marker on its own
series leaves every per-detector metric with a stable label set.

To filter the per-detector metrics down to the winner, join on the shared
labels, e.g.:

```promql
anomaly_score * on (id, group, detector, instance) group_left anomaly_best_detector
```

## Inspect Before Switching

Check confidence side-by-side:

```promql
anomaly_confidence_score{id="requests"}
```

Recommended workflow: run in `"explicit_plus_best"` mode first, verify the winner’s confidence is clearly higher, then flip to `true`.

## Cost

~4 extra detector runs per series per detector per day. On a 1k-series group with 3 detectors this is negligible compared to normal scrape load.