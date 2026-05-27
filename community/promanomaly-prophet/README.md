# promanomaly-prophet

Prophet-residual anomaly detector for [promanomaly](https://github.com/esops-dev/promanomaly).

Registers `ProphetResidual` via the `promanomaly.detectors` entry point, demonstrating the third-party plug-in path documented in the [detectors guide](../../docs/detectors.md#third-party-detectors-via-entry-points).

## What it does

For each detector run, fits a Prophet model on the rolling window and scores the latest sample by its residual divided by Prophet's uncertainty band half-width. Score units are roughly comparable to MAD/Hampel (number of band-widths from the predicted point), and the standard `alert_thresholds.score` knob controls firing.

## Install

```bash
pip install promanomaly promanomaly-prophet
```

Prophet itself depends on `cmdstan` / `pystan` — installation can be slow but works against Python 3.12 wheels for `prophet>=1.1`.

## Configure

```yaml
groups:
  - name: capacity
    queries:
      - id: request_rate
        promql: 'rate(http_requests_total{job="api"}[5m])'
        detectors:
          - name: ProphetResidual
            params:
              changepoint_prior_scale: 0.05   # default
              interval_width: 0.95            # default
```

## When to choose this

Best for signals with learnable seasonality (daily / weekly) where the residual from a forecast is informative as an anomaly signal. Sibling tool [promforecast](https://github.com/esops-dev/promforecast) is generally the right home for forecast-driven anomaly detection at scale — this detector exists as a lightweight escape hatch when the operator already runs promanomaly and wants to mix one Prophet-scored query into a group of statistical detectors.

## License

Apache-2.0 — same as promanomaly.
