# Per-Detector Confidence and Baseline Stability

promanomaly exposes two extra gauges that answer the question:  
**“How much should I trust this detector on this series?”**

Use them to filter alerts and cut noise before you page.

| Metric                        | Range | What it tells you                                      |
|-------------------------------|-------|--------------------------------------------------------|
| `anomaly_confidence_score`    | 0–1   | Overall trustworthiness of the detector for this series |
| `anomaly_baseline_stability`  | 0–1   | How stable the rolling baseline is                     |

Both gauges carry the same labels as `anomaly_score` (`id`, `group`, `detector`, `detector_instance`, plus any surviving source labels).

## When Are They Emitted?

They appear whenever calibration runs for a series + detector pair. Calibration happens when **either**:

- `auto_select` is enabled on the group (`true` or `"explicit_plus_best"`), **or**
- `defaults.emit_baseline_stability: true` (this is the default)

Turn off calibration completely with `defaults.emit_baseline_stability: false` if you don’t need these gauges.

## Recommended Use in Alerts

Join them with `and on (...)` to gate your alerts:

```promql
anomaly_outside_threshold == 1
  and on (id, group, detector)
    anomaly_confidence_score > 0.7
  and on (id, group, detector)
    anomaly_baseline_stability > 0.6
```

**Quick starting thresholds**  
- `confidence_score > 0.5` → drops detectors that perform poorly on synthetic tests  
- `baseline_stability > 0.5` → drops series with fast-drifting baselines

## How the Scores Are Calculated

### Baseline Stability
Compares the first and second half of the window:

$$
\text{stability} = \text{clamp}\left(1 - \frac{|b_2 - b_1|}{\max(|b_1|, |b_2|, 10^{-9})}, 0, 1\right)
$$

A barely-changing baseline scores near 1.0; a doubling baseline scores near 0.

### Confidence Score
Weighted combination refreshed every `auto_select_interval` (default 24 h):

$$
\begin{align*}
\text{precision} &= \frac{TP}{TP + FP} \\[4pt]
\text{confidence} &= \text{clamp}\Bigl(0.4 \cdot \text{precision} \\
&\quad + 0.3 \cdot \text{baseline_stability} \\
&\quad + 0.3 \cdot \text{coverage}, 0, 1\Bigr)
\end{align*}
$$

`coverage` = fraction of expected samples actually present.

## Operational Notes

- Gauges live in memory and are **cleared on restart**.
- They reappear after the next calibration cycle.
- For fail-open behaviour during the gap (useful on high-severity alerts):

```promql
anomaly_outside_threshold == 1
  and on (id, group, detector) (
    anomaly_confidence_score > 0.7 or absent(anomaly_confidence_score) == 1
  )
```

That’s it — two simple gauges that turn raw detector output into production-grade, trustworthy alerts.