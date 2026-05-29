# CLI Reference

promanomaly ships with a small, focused set of CLI commands for validation, testing, debugging, and offline analysis. Flags are aligned with promforecast where the meaning is the same.

## Available Commands

| Command             | Purpose |
|---------------------|---------|
| (default)           | Start the production server |
| `validate`          | Validate config (optionally probe datasource) |
| `dry-run`           | Validate + run one full detection cycle, then exit |
| `detect-once`       | Run one detector on an ad-hoc PromQL query |
| `analyze`           | Offline change-point analysis over historical data |
| `calibrate-buckets` | Suggest the right stratified-baseline bucketing for a signal |
| `detectors list`    | List registered detectors |
| `inspect`           | Inspect one running detector's state for an (id, label) series |
| `top`               | List the currently-firing series, ranked by severity |

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

## `calibrate-buckets`

Suggests the right stratified-baseline bucketing for a signal. Runs an autocorrelation analysis over the historical range and recommends `HourOfDayMAD` or `DayOfWeekMAD` (with parameters) based on the dominant period. Outputs a ready-to-paste detector config snippet so you don't have to guess between `lookback: 7d` and `lookback: 4w`.

```bash
promanomaly calibrate-buckets \
  --query 'rate(http_requests_total[5m])' \
  --from -14d

promanomaly calibrate-buckets \
  --query 'rate(http_requests_total{job="api"}[5m])' \
  --from -4w --step 10m --output json
```

**Flags**

| Flag                | Default     | Description |
|---------------------|-------------|-------------|
| `--query <promql>`  | required    | PromQL expression |
| `--from <time>`     | required    | Start time (RFC3339, unix, or `-14d`); should cover at least two full candidate periods (e.g. `-14d` to surface a 7d signal) |
| `--to <time>`       | now         | End time |
| `--step <dur>`      | 5m          | Sample step for the range query |
| `--output`          | text        | `text` (prints recommendation + YAML snippet) or `json` (raw analysis) |
| `--datasource-url`  | http://localhost:8428/ | Datasource URL |
| `--timeout`         | 30s         | Datasource HTTP timeout |

**When the tree says "no stratified detector clearly fits"**, the signal probably doesn't benefit from stratification — use plain `MAD` / `Hampel` against the rolling window. See [`scaling-stratified.md`](scaling-stratified.md) for the operational rationale and TSDB-cost projections.

## `detectors list`

Lists all installed detectors with name, description, and parameters.

```bash
promanomaly detectors list
promanomaly detectors list --target http://localhost:9092   # query a running instance
```

## `inspect`

The highest-leverage answer to "why did this series score 4.7?" — queries a running detector's `/debug/inspect` endpoint, which re-fetches the rolling window from the TSDB right now, re-runs every configured detector against it, and returns the per-detector score, baseline, threshold, and (when calibrated) confidence + stability.

```bash
promanomaly inspect --target http://localhost:9092 --id error_rate
promanomaly inspect --target http://localhost:9092 --id error_rate --labels 'instance=host-a'
promanomaly inspect --target http://localhost:9092 --id error_rate --output json
```

**Flags**

| Flag                | Default     | Description |
|---------------------|-------------|-------------|
| `--target <url>`    | required    | Running detector base URL (e.g. `http://localhost:9092`) |
| `--id <id>`         | required    | Metric id (matches a configured `query.id`) |
| `--labels <sel>`    | none        | Label selector (`k1=v1,k2=v2`); empty matches every series for the id |
| `--output`          | text        | `text` or `json` |
| `--timeout <s>`     | 10          | HTTP timeout |

The `/debug/inspect` endpoint is part of the same `/debug/*` family covered by the bundled NetworkPolicy — by default only same-namespace pods can reach it.

**Note on dynamic discovery.** `inspect` matches against the configured `query.id`. For queries with a `discover:` block, the templated form (`cpu_busy_{{ instance }}`) is the configured id, so the endpoint can't currently resolve a rendered id like `cpu_busy_node_17`. Use `detect-once` for ad-hoc inspection of discovery-expanded series.

## `top`

Answers "what is anomalous right now, ranked?" without opening Grafana — like `top` on a host, but for the fleet. Queries a running detector's `/debug/anomalies` endpoint (read from the in-memory snapshot, so no extra TSDB queries) and prints the currently-firing series ranked by severity.

```bash
promanomaly top --target http://localhost:9092
promanomaly top --target http://localhost:9092 --group node_cpu --min-severity 0.5
promanomaly top --target http://localhost:9092 --output json
```

Pairs with `inspect` as a two-step triage flow: `top` to find the worst series, `inspect` to understand *why* it scored. The `/debug/anomalies` endpoint is part of the `/debug/*` family the bundled NetworkPolicy denies by default. See [triage.md](triage.md) for the full surface.