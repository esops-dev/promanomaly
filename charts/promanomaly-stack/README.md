# promanomaly-stack umbrella chart

Greenfield bundle: installs the [promanomaly](../promanomaly/README.md)
detector plus a VictoriaMetrics single-node TSDB in one ``helm
install``. Production users running an existing TSDB should install
the ``promanomaly`` chart on its own and point its ``datasource.url``
at their long-term TSDB instead.

```bash
helm repo add promanomaly https://esops-dev.github.io/promanomaly
helm dependency update charts/promanomaly-stack
helm install obs promanomaly/promanomaly-stack
```

Subchart values are nested under each subchart's name in
``values.yaml``:

```yaml
promanomaly:
  groups:
    - name: example
      queries:
        - id: up_count
          promql: count(up == 1)
          detectors:
            - name: MAD

victoria-metrics-single:
  server:
    retentionPeriod: 30
```
