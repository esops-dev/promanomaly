# CLI Reference

promanomaly ships with a small, focused set of CLI commands for validation, testing, debugging, and offline analysis. Flags are aligned with promforecast where the meaning is the same.

## Available Commands

| Command             | Purpose |
|---------------------|---------|
| (default)           | Start the production server |
| `validate`          | Validate config (optionally probe datasource) |
| `generate-rules`    | Scaffold a PrometheusRule from a config (selectors filled, thresholds TODO) |
| `dry-run`           | Validate + run one full detection cycle, then exit |
| `detect-once`       | Run one detector on an ad-hoc PromQL query |
| `analyze`           | Offline change-point analysis over historical data |
| `calibrate-buckets` | Suggest the right stratified-baseline bucketing for a signal |
| `detectors list`    | List registered detectors |
| `inspect`           | Inspect one running detector's state for an (id, label) series |
| `top`               | List the currently-firing series, ranked by severity |
| `warmup`            | Report which series are still warming up, with an ETA to readiness |
| `diagnose`          | Flag mis-tuned detectors (never-fires, fires-often, stuck warming, empty) |
| `timeline`          | Post-incident anomaly timeline over a historical window |
| `adapter`           | Run the Kubernetes external/custom metrics adapter |
| `adapter-validate`  | Validate an adapter config file (schema only) |

All commands accept `--log-level`. `--config` is required only for server, `validate`, `generate-rules`, and `dry-run`.

## `validate`

Validates the YAML configuration against the schema. With `--probe` it also executes the queries against the live datasource; with `--estimate-cost` it statically projects the config's cardinality and query/compute cost without touching the datasource; with `--lint-metadata` it checks each query's metric type against `/api/v1/metadata`. All three compose — pass them together to check that the queries return data, fit the caps, *and* aren't feeding a raw counter to a detector.

```bash
promanomaly validate --config config.yaml
promanomaly validate --config config.yaml --probe --strict
promanomaly validate --config config.yaml --estimate-cost --strict
promanomaly validate --config config.yaml --lint-metadata --strict
```

**Flags**

| Flag                | Default      | Description |
|---------------------|--------------|-------------|
| `--config <path>`   | required     | Path to YAML config |
| `--probe`           | off          | Execute queries against datasource |
| `--estimate-cost`   | off          | Statically project cardinality, TSDB query load, and a coarse CPU/memory estimate (no datasource access) |
| `--lint-metadata`   | off          | Query `/api/v1/metadata` and warn when a query feeds a raw counter to a detector without `rate()`/`increase()` |
| `--strict`          | off          | With `--probe`: exit non-zero if any query returns no data or errors. With `--estimate-cost`: exit non-zero if projected series exceed `safety.max_total_series`. With `--lint-metadata`: exit non-zero if any mismatch is found |
| `--datasource-url`  | from config  | Override datasource URL |

### `--lint-metadata` output

One `{"status": "lint", ...}` JSON line per finding (`group`, `query`, `metric`, `metric_type`, `issue`, `detectors`, `message`) plus a `{"summary": ...}` line with `metrics_with_metadata` and `findings` counts. The only check today is `counter_not_rated`: a metric the TSDB reports as a `counter` that appears in a query without a rate-like wrapper (`rate`, `irate`, `increase`, `delta`, `idelta`, `deriv`, `resets`). A counter used only inside a series-identity aggregation (`count`, `count_values`, `group`, `absent`, …) is *not* flagged — e.g. `node_load1 / count(node_cpu_seconds_total)` counts cores and never scores the raw counter. **Lint only** — it never rewrites the query. Absent metadata is treated as "no opinion", so a metric the TSDB carries no metadata for never produces a false positive (handles VictoriaMetrics/Mimir/Thanos endpoint quirks). The same finding is surfaced live by `inspect` and `top --lint`. Flag-name parity with promforecast.

### `--estimate-cost` output

One JSON line per group plus a summary line. Per group: `projected_series` (worst case at `safety.max_series_per_query`), `short_queries_per_refresh`, `discovery_probe_queries_per_refresh`, `stratified_baseline_queries_per_day`, `estimated_memory_bytes`, and `estimated_cpu_cores`. The summary aggregates the totals and flags `over_budget` against `safety.max_total_series`. The CPU figure is anchored to the documented "~10k series at a 1-minute refresh on one 1-CPU replica" model — an order-of-magnitude sizing aid, not a benchmark. Pairs with `--probe` (which confirms the queries actually return data); see [production-checklist.md](production-checklist.md).

## `generate-rules`

Scaffolds a `PrometheusRule` manifest from a config so you don't hand-copy `id`/`group` label sets. Emits one `AnomalyOutsideThreshold`-style alert per group/query with the selectors already filled in, plus the global `max`-ensemble recording-rule fallback (`anomaly_outside_threshold_any`) described in [patterns.md](patterns.md). Groups that set an `ensemble:` block also get an ensemble-agreement alert.

```bash
promanomaly generate-rules --config config.yaml                 # to stdout
promanomaly generate-rules --config config.yaml --output rules.yaml
promanomaly generate-rules --config config.yaml --name my-team-anomalies
```

Severities, `for:` windows, and any runbook links are emitted as clearly-marked `# TODO` placeholders — the project ships **no opinionated thresholds**, so this is scaffolding, not a recommendation. Each alert gets a unique, descriptive name (derived from group + query id) so the file is duplicate-rule-lint clean (`promtool check rules`) and each alert is individually routable / silenceable in Alertmanager. Discover-templated query ids can't be pinned to a single literal `id=`, so those alerts scope by `group` only and carry a note to narrow the selector once the expansions are known.

The intended workflow pairs with the GitOps validation Action: **generate, edit the `TODO`s, validate, commit.**

**Flags**

| Flag                | Default                | Description |
|---------------------|------------------------|-------------|
| `--config <path>`   | required               | Path to YAML config |
| `--output <path>`   | stdout                 | Write the manifest to a file instead of stdout |
| `--name <name>`     | `promanomaly-generated`| `metadata.name` for the generated PrometheusRule |

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
promanomaly top --target http://localhost:9092 --no-lint   # skip the extra metadata query
promanomaly top --target http://localhost:9092 --output json
```

By default `top` also prints metadata-lint hints (the same counter-not-rated warning `validate --lint-metadata` reports), which costs one extra `/api/v1/metadata` query against the TSDB; pass `--no-lint` to skip it. Pairs with `inspect` as a two-step triage flow: `top` to find the worst series, `inspect` to understand *why* it scored. The `/debug/anomalies` endpoint is part of the `/debug/*` family the bundled NetworkPolicy denies by default. See [triage.md](triage.md) for the full surface.

## `warmup`

Answers "the dashboard is empty after `helm install` — do I wait or debug?" Queries a running detector's opt-in `/warmup` endpoint (enable it with `server.expose_warmup_endpoint: true`) and reports, per group and query, how many rolling-window samples are available versus `min_points`, an ETA to readiness at the current step, and the blocker.

```bash
promanomaly warmup --target http://localhost:9092
promanomaly warmup --target http://localhost:9092 --output json
```

**Flags**

| Flag              | Default | Description |
|-------------------|---------|-------------|
| `--target <url>`  | required | Running detector base URL |
| `--output`        | text    | `text` (aligned table) or `json` (raw `/warmup` response) |
| `--timeout`       | 30      | HTTP timeout in seconds |

**`blocked_by` values**: `none` (ready), `min_points` (some samples, below the threshold), `lookback` (no samples yet), `baseline` (a stratified detector still filling its multi-week lookback), `discovery` (a templated query that can't be probed until discovery resolves).

The exit code is **0** when every query in every group is `ready` (or there are no queries to warm up) and **1** when at least one is still warming or errored — so a CI pre-flight can gate "is the install done?" on `until promanomaly warmup --target ...`. Network failures exit **2**. The endpoint is in-namespace only (the bundled NetworkPolicy denies external ingress alongside `/debug/*`). Mirrors `promforecast warmup`.

## `diagnose`

Answers "which of my detectors are mis-tuned?" from evidence, not guesswork. Two modes share one report shape:

- **TSDB lookback** (`--datasource-url`) — aggregates the detector's own scraped metrics over `--window` to flag the firing-rate bands (**never fires** = dead config; **fires often** = threshold too low / wrong detector), **chronically-warming** series, and **empty queries**. With `--config` it also flags **silent detectors** — a configured detector that emitted no score over the window, which includes a cohort detector dropping every cohort below `min_cohort_size`.
- **live snapshot** (`--target`) — one `/metrics` scrape for a current-state read (firing now, warming now, empty now). A single snapshot can't establish a rate over time, so it never reports `never_fires` — use the TSDB mode for that.

Pass exactly one of `--target` / `--datasource-url`.

```bash
# Over a 24h lookback against the TSDB scraping the detector:
promanomaly diagnose --datasource-url http://victoriametrics:8428 --window 24h
promanomaly diagnose --datasource-url http://victoriametrics:8428 --config config.yaml --output json

# Quick current-state read straight off a running detector:
promanomaly diagnose --target http://localhost:9092
```

**Flags**

| Flag                | Default | Description |
|---------------------|---------|-------------|
| `--target <url>`    | —       | Running detector base URL (snapshot mode) |
| `--datasource-url`  | —       | TSDB holding the detector's scraped metrics (lookback mode) |
| `--window <dur>`    | 24h     | Lookback for the TSDB analysis (Prometheus duration) |
| `--config <path>`   | none    | With `--datasource-url`, cross-check configured detectors for silent ones |
| `--fire-high <f>`   | 0.5     | Firing-rate fraction at/above which a detector is flagged too noisy |
| `--output`          | text    | `text` (grouped by category) or `json` (for CI) |
| `--timeout <s>`     | 30      | HTTP timeout |

Pure analysis, no persisted state. Pairs with `backtest` (precision/recall on injected anomalies) and `calibrate-buckets` (stratified bucketing) to close the tuning loop. Parity with `promforecast diagnose`.

## `timeline`

Post-incident anomaly timeline. Queries the TSDB for `anomaly_outside_threshold`, `anomaly_change_point_total`, and `anomaly_severity` history and prints a time-ordered list of anomaly events — every firing start and every change-point, with severity attached. Complements `analyze` (which finds change-points in a *raw signal*) by operating on the *emitted anomaly metrics* — the retro view.

```bash
promanomaly timeline --from -6h --to now
promanomaly timeline --from 2025-05-29T08:00:00Z --to 2025-05-29T12:00:00Z --group api --group web
promanomaly timeline --from -24h --output json
```

**Flags**

| Flag               | Default                    | Description |
|--------------------|----------------------------|-------------|
| `--from <ts>`      | (required)                 | Range start (RFC3339, unix seconds, or relative e.g. `-6h`) |
| `--to <ts>`        | `now`                      | Range end |
| `--group <name>`   | all                        | Restrict to these groups (repeatable) |
| `--datasource-url` | `http://localhost:8428/`    | PromQL-compatible datasource URL |
| `--step`           | `15s`                      | Sample step for the range queries |
| `--output`         | `text`                     | `text` (human table) or `json` (machine-readable list of events) |
| `--timeout`        | `30s`                      | Datasource HTTP timeout |

Each event carries: `time`, `time_rfc3339`, `kind` (`anomaly` or `change_point`), `id`, `group`, `detector`, `detector_instance`, and `severity` (when available). Events are sorted by ascending time; ties break by kind then `id` for a deterministic order.

## `adapter`

Run the Kubernetes external/custom metrics adapter — a separate, stateless process from the detector that re-serves the anomaly metrics already in the TSDB through `external.metrics.k8s.io` / `custom.metrics.k8s.io`, so HPA and KEDA can consume anomaly signal. It is normally deployed via the [`promanomaly-metrics-adapter`](../charts/promanomaly-metrics-adapter/) chart, which generates this config and wires the serving TLS + APIService registrations. See [adapter.md](adapter.md) for the full surface and the autoscaling guard-rails.

```bash
promanomaly adapter --config adapter.yaml
promanomaly adapter-validate --config adapter.yaml   # schema only
```

The adapter takes its **own** config file (datasource, listen address, serving TLS, the metric allow-list). It shares the datasource auth shapes (`none`/`bearer`/`basic`/`mtls`) with the detector. `adapter-validate` checks the schema and prints the resolved listen address, TLS state, and exposed metrics — suitable as a CI pre-deploy gate.