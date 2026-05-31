# promanomaly-stack Umbrella Chart

Greenfield bundle that installs the **promanomaly detector** + a **VictoriaMetrics single-node TSDB** in one `helm install`.

> **Production note**: If you already have a VictoriaMetrics (or any Prometheus-compatible TSDB), install the plain [`promanomaly` chart](../promanomaly/README.md) instead and point `datasource.url` at your existing long-term store.

## Quick Start

```bash
helm repo add promanomaly https://esops-dev.github.io/promanomaly

helm dependency update charts/promanomaly-stack

helm install obs promanomaly/promanomaly-stack \
  --namespace monitoring \
  --create-namespace
```

## Configuration

Subchart values are nested under each component:

```yaml
promanomaly:
  datasource:
    url: http://{{ include "victoria-metrics-single.fullname" . }}:8428/
  groups:
    - name: example
      queries:
        - id: up_count
          promql: count(up == 1)
          detectors:
            - name: MAD

victoria-metrics-single:
  server:
    retentionPeriod: 30d          # adjust as needed
    # ... other VM settings
```

Full reference is in `charts/promanomaly-stack/values.yaml`.

That’s it — one command gives you a working detector + storage for immediate testing or small-scale production. For larger setups, switch to the standalone `promanomaly` chart with your own TSDB.