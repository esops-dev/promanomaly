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
| `true`                        | Only the winner emits; carries `best_detector=<name>` |
| `"explicit_plus_best"`        | All detectors emit + winner is tagged (great for tuning) |

## How It Works

Every `auto_select_interval` (default 24h) the detector runs a quick calibration per series:

1. Scores the real rolling window → counts **false positives**.
2. Injects three synthetic anomalies (point spike, step change, level drift) → counts **true positives** and **false negatives**.
3. Computes precision, recall, and F1 score.

Highest F1 wins. Ties broken by precision, then by order in the config.

## Output

When `auto_select: true`, only the winner’s metrics appear:

```
anomaly_score{..., detector="MAD", best_detector="MAD", ...} 4.7
```

The `best_detector` label is only present when auto-select is enabled.

## Inspect Before Switching

Check confidence side-by-side:

```promql
anomaly_confidence_score{id="requests"}
```

Recommended workflow: run in `"explicit_plus_best"` mode first, verify the winner’s confidence is clearly higher, then flip to `true`.

## Cost

~4 extra detector runs per series per detector per day. On a 1k-series group with 3 detectors this is negligible compared to normal scrape load.