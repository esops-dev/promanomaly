# Severity Signal

`anomaly_severity{id, group, detector, detector_instance?, ...}` is a normalized **0–1** gauge that answers the practical on-call question:  
**“How much should I care about this anomaly compared to everything else firing right now?”**

Raw `anomaly_score` values are not comparable across detectors (MADs vs. probability vs. sigma). Severity folds them into one consistent scale for triage and ranking.

It is **enrichment**, not the alerting decision. You still alert on `anomaly_outside_threshold` or `anomaly_severity` directly.

## Formula

\[
\text{severity} = \text{strength} \times (0.40 + 0.30 \times \text{confidence} + 0.15 \times \text{stability} + 0.15 \times \text{duration_ramp})
\]

Severity is **0** when the series is not firing (below threshold).

### Components

| Term             | Range | Source                                      |
|------------------|-------|---------------------------------------------|
| `strength`       | 0–1   | Normalized `anomaly_score` (see below)      |
| `confidence`     | 0–1   | `anomaly_confidence_score` (defaults to 1.0 before calibration) |
| `stability`      | 0–1   | `anomaly_baseline_stability` (defaults to 1.0 before calibration) |
| `duration_ramp`  | 0–1   | `min(anomaly_duration_seconds / 1800, 1)` — saturates after 30 min |

**Strength calculation**  
- Probability detectors (BOCPD): `strength = score`  
- All others: `strength = clamp(score / (2 × threshold), 0, 1)`

This makes a 6-MAD spike (default threshold = 3) comparable to a 1.0-probability change-point.

## Worked Example

MAD detector (`threshold = 3.0`) scores 4.5, `confidence = 0.8`, `stability = 0.6`, firing for 15 minutes:

\[
\begin{align*}
\text{strength} &= \clamp(4.5 / 6.0, 0, 1) = 0.75 \\
\text{duration_ramp} &= \min(900 / 1800, 1) = 0.5 \\
\text{multiplier} &= 0.40 + 0.30 \times 0.8 + 0.15 \times 0.6 + 0.15 \times 0.5 = 0.805 \\
\text{severity} &\approx 0.60
\end{align*}
\]

## Usage

**High-severity alerting** (recommended):

```promql
anomaly_severity >= 0.7
```

The bundled reference alert `AnomalyHighSeverity` uses `0.7` by default.

**Live triage**:

```bash
promanomaly top --target http://localhost:9092 --min-severity 0.5
```

## Configuration

```yaml
defaults:
  emit_severity: true   # default
```

Set to `false` to suppress the gauge.

The weights are deliberately **not user-tunable** — operators who want a
different weighting compose via recording rules, keeping the detector
free of tuning sprawl. Severity is signal enrichment, distinct from
`anomaly_confidence_score` (a single detector's quality) and
`anomaly_composite_score` (cross-detector fusion).

## Cross-tool mirroring (promforecast)

Severity uses only the contracted `id` / `group` labels plus
promanomaly-private labels (`detector`, `detector_instance`), so it
introduces **no new shared label** and requires no change to
[`LABELS_CONTRACT.md`](../LABELS_CONTRACT.md).

The decision on whether promforecast mirrors a `forecast_severity` of the
same shape is **deferred to promforecast's own roadmap**: a forecast
deviation's "severity" inputs (band width, horizon, deviation magnitude)
differ enough from an anomaly score's that a shared formula would be
forced rather than natural. If promforecast later adopts a parallel
gauge, any shared label it introduces lands via a coordinated PR pair
against the contract in both repos. Until then, the two compose in
PromQL on `(id, group)` exactly as today.

---

Severity gives you a single, human-readable ranking of active anomalies without having to understand each detector’s raw units. It composes cleanly with promforecast outputs via the shared `(id, group)` labels.