# promanomaly detector

Python source for the promanomaly detector. See the
[repository root README](../README.md) for the project overview, and
[`../ROADMAP.md`](../ROADMAP.md) for the milestone plan.

## Layout

```
src/promanomaly/
  main.py           # FastAPI app + scheduler lifecycle
  cli.py            # promanomaly CLI (validate, dry-run, detect-once)
  config.py         # pydantic config schema
  source.py         # PromQL range-query client
  runner.py         # per-group, per-series detection pipeline
  state.py          # snapshot cache, warm-up & duration counters
  exporter.py       # Prometheus exposition + metric-name validator
  logging.py        # structlog setup
  detectors/
    base.py         # Detector Protocol + DetectorResult
    registry.py     # entry-point-based detector discovery
    mad.py          # MAD (Median Absolute Deviation) detector
    hampel.py       # Hampel filter detector
```

Tests live in `tests/`.

## Development

From the repository root:

```bash
make install     # uv sync --all-extras
make lint        # ruff + mypy --strict
make test        # pytest
```
