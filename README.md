# promanomaly

Anomaly detection for Prometheus-compatible metrics, exposed as Prometheus metrics.

[![CI](https://github.com/esops-dev/promanomaly/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/esops-dev/promanomaly/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/esops-dev/promanomaly?include_prereleases&sort=semver)](https://github.com/esops-dev/promanomaly/releases)
[![License](https://img.shields.io/github/license/esops-dev/promanomaly)](LICENSE)

promanomaly reads a YAML config, queries a Prometheus-compatible TSDB
(VictoriaMetrics by default), runs robust statistical detectors over a
rolling window of recent data, and exposes anomaly scores on `/metrics`.
Anomalies become first-class metrics — graphable in Grafana, alertable
through Alertmanager, indistinguishable from anything else in your
monitoring stack.

It is the sibling project to [**promforecast**](https://github.com/esops-dev/promforecast).
Where promforecast does forecast-based anomaly detection on signals with
learnable seasonality, promanomaly handles everything else: change-points,
statistical baselines on signals that aren't reasonably forecastable, and
(later) population/cohort divergence. Install one, both, or neither — they
share architectural DNA but no runtime dependency.

## Why

- **Catches what forecasting misses.** Step changes after a deploy, spikes
  on sparse signals, and series diverging from their cohort — none of
  which forecast-based deviation alerts pick up reliably.
- **Drop-in.** `helm install` next to an existing Prometheus and point it
  at a long-term TSDB. No production scrape changes required.
- **Speak Prometheus everywhere.** PromQL in, exposition format out. The
  TSDB is swappable (VictoriaMetrics, Mimir, Thanos).
- **Anomalies are just metrics.** Same labels, same retention, same
  dashboards. Compose with recording rules and alerts you already know.
- **Stateless.** No database to operate. Baselines recompute on a
  schedule; results live in `/metrics`.
- **Cheap per series.** Detectors are O(N log N) on a rolling window with
  no model fitting — roughly an order of magnitude lighter than a
  statistical forecast.

## Quickstart

```bash
helm repo add promanomaly https://esops-dev.github.io/promanomaly
helm install promanomaly promanomaly/promanomaly-stack
```

This installs VictoriaMetrics, the detector, and example dashboards.
For an existing TSDB, install only `promanomaly/promanomaly`.

Editing the YAML config does **not** require a pod restart: the detector
watches the mounted ConfigMap, validates the new config in-place, and
rolls back on validation failure (`kubectl apply` to the ConfigMap is
enough). `POST /-/reload` and `SIGHUP` trigger the same path. Run
`promanomaly validate --config path/to/config.yaml` in CI to catch
schema errors before merging — see [`docs/operations.md`](docs/operations.md)
for the full CLI surface and operational guidance.

See [`docs/`](docs/) for configuration, detector selection, and
operational guidance — including [`docs/patterns.md`](docs/patterns.md),
the composition cookbook of how anomaly scores plug into the rest of
the Prometheus ecosystem (rate-of-change, group rollups, multi-window
monitoring, deploy-time silencing, cross-tool joins with promforecast).
A self-monitoring example config is bundled at
[`examples/configs/self-monitoring.yaml`](examples/configs/self-monitoring.yaml),
and a starter `PrometheusRule` at
[`examples/alerts/promanomaly-rules.yaml`](examples/alerts/promanomaly-rules.yaml).
Full multi-group production reference configs land in a later release.

## Relationship with promforecast

| | promforecast | promanomaly |
|---|---|---|
| Question | "What will this look like in N hours?" | "Is this abnormal **right now**?" |
| Method | Fit a model on a long lookback, predict ahead. | Compute a baseline on a short rolling window, score the present. |
| Latency | Minutes-to-hours (next refit). | Seconds-to-minutes (next scrape). |
| Best for | Capacity, traffic, KPIs. | Bursty, sparse, step-shifting, or fleet-relative signals. |

Run both for full coverage: forecast-based deviation alerts on capacity
metrics, plus change-point and baseline anomaly alerts on error rates,
rare events, and per-instance outliers.

## Status

Early development. Output metric names are part of the public API and
follow Prometheus conventions strictly (no colons — those are reserved
for recording rules).

## Security

Found a vulnerability? See [SECURITY.md](SECURITY.md). Please do not open public issues for security reports.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and the [docs/](docs/) folder for detector authoring.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
