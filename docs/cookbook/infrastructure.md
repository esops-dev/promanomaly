# Infrastructure Signals

Recipes for common node-exporter-style host metrics.  
The complete ready-to-use bundle is in [`examples/configs/node-exporter.yaml`](../../examples/configs/node-exporter.yaml).  
Copy only the signals you need — every config fans out automatically over the `instance` label.

---

## Node Filesystem Fill

**Why it matters.** A full disk kills services instantly. The dangerous case is a sudden step increase that stays high (log explosion or runaway write).

**Recommended detectors.** `MAD` for jumps + `CUSUM` for persistent regime shifts.

```yaml
apiVersion: promanomaly.io/v1
datasource:
  url: http://victoriametrics:8428/
defaults:
  window: 1h
  step: 15s
groups:
  - name: node_filesystem
    queries:
      - id: node_filesystem_used_fraction
        promql: |
          1 - (
            node_filesystem_avail_bytes{fstype!~"tmpfs|overlay"}
            / node_filesystem_size_bytes{fstype!~"tmpfs|overlay"}
          )
        detectors:
          - name: MAD
          - name: CUSUM
```

**Dashboard shape.** Used-fraction line with `anomaly_baseline` just below it. A step change appears as a flat shift the baseline hasn’t caught up to yet.

**Cardinality.** One series per `(instance, device, mountpoint)`. On 100 nodes with 3 mounts ≈ 300 series per detector.

**Example alert.**
```yaml
- alert: FilesystemFillingAnomaly
  expr: anomaly_outside_threshold{id="node_filesystem_used_fraction"} == 1
  for: 10m
```

---

## CPU Saturation

**Why it matters.** Sustained high CPU queues requests and spikes latency. The subtle case is “one node hot while the rest are fine”.

**Recommended detectors.** `MAD` for per-host spikes + `Cohort` for fleet-wide divergence.

```yaml
apiVersion: promanomaly.io/v1
datasource:
  url: http://victoriametrics:8428/
defaults:
  window: 1h
  alert_thresholds:
    score: 4.0
groups:
  - name: node_cpu
    queries:
      - id: node_cpu_busy
        promql: 1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m]))
        detectors:
          - name: MAD
          - name: Cohort
            instance: fleet
            params:
              cohort_label: instance
              min_cohort_size: 5
```

**Dashboard shape.** Per-host busy fraction; Cohort firings highlight the single outlier diverging from the peer band.

**Cardinality.** One series per host per detector.

**Example alert.**
```yaml
- alert: CPUSaturationAnomaly
  expr: anomaly_outside_threshold{id="node_cpu_busy", detector="Cohort"} == 1
  for: 10m
```

---

## Memory Pressure / OOM Risk

**Why it matters.** Falling available memory is the leading indicator of OOM kills.

**Recommended detector.** `MAD` on available fraction (pair with OOM counter rate from the Kubernetes cookbook if available).

```yaml
apiVersion: promanomaly.io/v1
datasource:
  url: http://victoriametrics:8428/
defaults:
  window: 1h
groups:
  - name: node_memory
    queries:
      - id: node_memory_available_fraction
        promql: node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes
        detectors:
          - name: MAD
```

**Dashboard shape.** Available fraction with baseline; firing on a sharp sustained dip.

**Cardinality.** One series per host.

**Example alert.**
```yaml
- alert: MemoryPressureAnomaly
  expr: anomaly_outside_threshold{id="node_memory_available_fraction"} == 1
  for: 5m
```

---

## Disk I/O Saturation

**Why it matters.** Saturated disks back up the entire I/O path and cause cascading latency.

**Recommended detector.** `MAD` — handles the naturally bursty I/O-time signal cleanly.

```yaml
apiVersion: promanomaly.io/v1
datasource:
  url: http://victoriametrics:8428/
defaults:
  window: 1h
groups:
  - name: node_disk_io
    queries:
      - id: node_disk_io_saturation
        promql: rate(node_disk_io_time_seconds_total[5m])
        detectors:
          - name: MAD
```

**Cardinality.** One series per `(instance, device)`.

**Example alert.**
```yaml
- alert: DiskIOSaturationAnomaly
  expr: anomaly_severity{id="node_disk_io_saturation"} >= 0.7
  for: 5m
```

---

## Network Error / Drop Rate

**Why it matters.** Rising errors or drops signal bad NICs, saturated links, or driver issues.

**Recommended detector.** `MAD` with a practical floor to ignore statistically-large but operationally-insignificant single-packet drops on quiet interfaces.

```yaml
apiVersion: promanomaly.io/v1
datasource:
  url: http://victoriametrics:8428/
defaults:
  window: 1h
groups:
  - name: node_network
    queries:
      - id: node_network_errs_drops
        promql: |
          rate(node_network_receive_errs_total[5m])
          + rate(node_network_receive_drop_total[5m])
          + rate(node_network_transmit_errs_total[5m])
          + rate(node_network_transmit_drop_total[5m])
        min_abs_delta: 1.0
        detectors:
          - name: MAD
```

**Cardinality.** One series per `(instance, device)`.

**Example alert.**
```yaml
- alert: NetworkErrorAnomaly
  expr: anomaly_outside_threshold{id="node_network_errs_drops"} == 1
  for: 10m
```