# promanomaly-iforest

Stateless Isolation-Forest anomaly detector for [promanomaly](https://github.com/esops-dev/promanomaly).

Refits an Isolation Forest on the rolling window every run. Nothing is persisted across runs or restarts, keeping it inside promanomaly's stateless promise.

## Install

```bash
pip install promanomaly promanomaly-iforest
```

## Usage

```yaml
detectors:
  - name: IsolationForest
    params:
      n_estimators: 100    # default
      contamination: 0.05  # default
```

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `n_estimators` | int | 100 | Number of trees in the isolation forest |
| `contamination` | float | 0.05 | Expected outlier proportion in the window |

## License

Apache-2.0
