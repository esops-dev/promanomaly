# CLI Reference

promanomaly ships with a small, focused set of CLI commands for validation, testing, debugging, and offline analysis. Flags are aligned with promforecast where the meaning is the same.

## Available Commands

| Command           | Purpose |
|-------------------|---------|
| (default)         | Start the production server |
| `validate`        | Validate config (optionally probe datasource) |
| `dry-run`         | Validate + run one full detection cycle, then exit |
| `detect-once`     | Run one detector on an ad-hoc PromQL query |
| `analyze`         | Offline change-point analysis over historical data |
| `detectors list`  | List registered detectors |

All commands accept `--log-level`. `--config` is required only for server, `validate`, and `dry-run`.

## `validate`

Validates the YAML configuration against the schema. With `--probe` it also executes the queries against the live datasource.

```bash
promanomaly validate --config config.yaml
promanomaly validate --config config.yaml --probe --strict
```

**Flags**

| Flag                | Default      | Description |
|---------------------|--------------|-------------|
| `--config <path>`   | required     | Path to YAML config |
| `--probe`           | off          | Execute queries against datasource |
| `--strict`          | off          | Exit non-zero if any query returns no data or errors |
| `--datasource-url`  | from config  | Override datasource URL |

## `dry-run`

Full validation + one complete detection run for every group. No HTTP server is started.

```bash
promanomaly dry-run --config config.yaml
```

## `detect-once`

The go-to tool for testing and tuning a single detector on a specific query.

```bash
promanomaly detect-once --query 'rate(http_requests_total[5m])' --detector MAD --window 1h

promanomaly detect-once --query 'rate(errors_total[5m])' --detector Hampel --param t0=5
```

**Flags**

| Flag                | Default                  | Description |
|---------------------|--------------------------|-------------|
| `--query <promql>`  | required                 | PromQL expression |
| `--detector`        | MAD                      | Detector name |
| `--window`          | 1h                       | Rolling window size |
| `--step`            | 15s                      | Query step |
| `--param k=v`       | —                        | Detector parameter override (repeatable) |
| `--datasource-url`  | http://localhost:8428/   | Datasource URL |
| `--timeout`         | 10s                      | Datasource timeout |

## `analyze`

Offline change-point detection on historical data using the `ruptures` library (different algorithm class from BOCPD/CUSUM).

```bash
promanomaly analyze \
  --query 'rate(http_requests_total{job="api"}[5m])' \
  --from 2026-05-25T00:00:00Z \
  --to 2026-05-25T06:00:00Z
```

**Flags**

| Flag                | Default     | Description |
|---------------------|-------------|-------------|
| `--query <promql>`  | required    | PromQL expression |
| `--from <time>`     | required    | Start time (RFC3339, unix, or `-24h`) |
| `--to <time>`       | now         | End time |
| `--detector`        | ruptures    | `ruptures` / `pelt` / `binseg` / `window` |
| `--output`          | text        | `text` or `json` |

## `detectors list`

Lists all installed detectors with name, description, and parameters.

```bash
promanomaly detectors list
promanomaly detectors list --target http://localhost:9092   # query a running instance
```