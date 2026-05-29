# Live Triage

When you get paged, the first question is always:  
**“What is actually anomalous right now, and what should I look at first?”**

`promanomaly top` and the `/debug/anomalies` endpoint give you a ranked list of current firings instantly — no Grafana, no custom PromQL.

## `promanomaly top`

```bash
promanomaly top --target http://localhost:9092
```

**Example output:**

```
  SEV     SCORE     DUR   TYPE               GROUP          ID / LABELS
------------------------------------------------------------------------
 0.82     5.31     12m   step_up            node_cpu       node_cpu_busy (MAD) {instance=host-17:9100}
 0.61     4.12      3m   cohort_divergence  node_cpu       node_cpu_busy (Cohort[fleet]) {instance=host-4:9100}
 0.44     3.05       -   point_spike        node_network   node_network_errs_drops (MAD) {instance=host-2:9100}
```

### Useful Flags

| Flag                    | Description |
|-------------------------|-------------|
| `--group <name>`        | Limit to one group |
| `--min-severity <0-1>`  | Hide low-severity firings |
| `--limit <n>`           | Show only the top N (ranked by severity) |
| `--output json`         | Raw JSON for scripting |

Pair it with `promanomaly inspect` for the two-step workflow:  
**`top`** → find the worst series → **`inspect`** → understand why it fired.

## `/debug/anomalies` Endpoint

The CLI is a thin wrapper around this internal endpoint:

```http
GET /debug/anomalies?group=<name>&min_severity=0.5&limit=10
```

**Response example:**

```json
{
  "count": 2,
  "min_severity": 0.5,
  "group": null,
  "anomalies": [
    {
      "id": "node_cpu_busy",
      "group": "node_cpu",
      "detector": "MAD",
      "detector_instance": null,
      "score": 5.31,
      "severity": 0.82,
      "duration_seconds": 720,
      "type": "step_up",
      "labels": { "instance": "host-17:9100" }
    }
  ]
}
```

Data comes directly from the in-memory snapshot (the same source as `/metrics`). No extra TSDB queries, always consistent with what Prometheus sees.

## Security

The shipped `NetworkPolicy` blocks ingress to all `/debug/*` endpoints by default (including `/debug/anomalies`, `/debug/inspect`, and `/debug/detectors`).  
Only open it if you need access from a bastion or operator tooling.