# GitOps Config Validation

Keep your promanomaly configuration in Git and validate it on every pull request. This prevents broken or overly expensive configs from ever reaching production.

The repository ships a composite GitHub Action (`action.yml`) that wraps [`promanomaly validate`](cli.md#validate).

## What It Checks

The action runs `promanomaly validate` with the checks you enable:

- **Schema validation** (always on) — config parses and matches the pydantic schema.
- **`--estimate-cost`** (default: on) — statically projects cardinality, TSDB query load, and rough CPU/memory usage. With `strict`, exceeding `safety.max_total_series` fails the job. Requires no live datasource.
- **`--probe`** (default: off) — executes every query against a live TSDB and fails on empty or errored results.
- **`--lint-metadata`** (default: off) — warns when raw counters are fed to detectors without `rate()` or `increase()`. Requires datasource access.

`strict` (default: `true`) turns any soft finding into a hard failure.

## Action Inputs

| Input              | Default      | Description |
|--------------------|--------------|-------------|
| `config`           | required     | Path to the config YAML file |
| `estimate-cost`    | `true`       | Enable static cardinality & cost projection |
| `probe`            | `false`      | Run live queries against datasource |
| `lint-metadata`    | `false`      | Enable counter-without-rate linting |
| `strict`           | `true`       | Fail on any issue |
| `datasource-url`   | from config  | Override datasource for probe/lint checks |
| `python-version`   | `3.12`       | Python version used by the validator |

## Sample Workflows

### Basic static validation (recommended for most PRs)

```yaml
name: Validate promanomaly config
on:
  pull_request:
    paths:
      - "promanomaly/**.yaml"

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: esops-dev/promanomaly@main
        with:
          config: promanomaly/config.yaml
          estimate-cost: "true"
          strict: "true"
```

### Full validation with live probe

```yaml
      - uses: esops-dev/promanomaly@main
        with:
          config: promanomaly/config.yaml
          probe: "true"
          lint-metadata: "true"
          strict: "true"
          datasource-url: https://staging-tsdb.internal/
```

Pin the action to a specific release tag (instead of `@main`) once you adopt a stable version.

## Pairing with `generate-rules`

This action pairs perfectly with [`promanomaly generate-rules`](cli.md#generate-rules):

1. Generate a PrometheusRule skeleton from your config.
2. Edit the `TODO` alert thresholds.
3. Validate the full config in CI with this action.
4. Commit the changes.

The workflow mirrors the one used by promforecast, so teams running both tools use identical CI patterns.