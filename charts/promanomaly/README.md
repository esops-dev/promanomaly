# promanomaly Helm chart

Installs the promanomaly detector — a single ``Deployment``, a
``Service`` for ``/metrics``, a ``ConfigMap`` for the detector
configuration, an optional ``ServiceMonitor`` for Prometheus Operator
setups, an optional ``PrometheusRule`` carrying the reference alerts,
and a ``NetworkPolicy`` that locks down ``/-/reload`` and the debug
endpoints by default.

Bring your own Prometheus/VictoriaMetrics. For a greenfield install
that bundles VictoriaMetrics, see the ``promanomaly-stack`` umbrella.

## Quick start

```bash
helm repo add promanomaly https://esops-dev.github.io/promanomaly
helm install my-detector promanomaly/promanomaly \
  --set datasource.url=http://my-victoriametrics:8428/ \
  --values my-groups.yaml
```

Where ``my-groups.yaml`` carries detector groups under ``groups:``.

## Values

See ``values.yaml`` and the schema in ``values.schema.json``. The
notable bits:

- ``replicaCount`` is pinned to ``1`` — single-replica is the only
  supported topology today. HA mode arrives in a later release.
- ``existingConfigMap`` lets GitOps tooling (Argo CD, Flux) manage the
  detector configuration out-of-band; the chart then skips its own
  ConfigMap rendering.
- ``networkPolicy.enabled`` defaults on; turn it off if your cluster
  does not use NetworkPolicies.
- ``serviceMonitor.honorLabels: true`` is the default and required
  for anomaly labels to flow through to Prometheus unchanged.
- ``prometheusRule.enabled`` is **off** by default — we don't ship
  opinionated thresholds. The bundled rules mirror
  ``examples/alerts/promanomaly-rules.yaml`` and expose
  per-rule toggles + tunable thresholds so operators can adopt the
  ones they want without templating from scratch.

## Linting locally

```bash
helm dependency update charts/promanomaly
helm lint --strict charts/promanomaly
helm template charts/promanomaly | kubeconform -strict -summary -ignore-missing-schemas
```
