# Contributing

Thanks for your interest in promanomaly.

## Before you start

- Open an issue first for non-trivial changes — design discussion is cheaper
  than re-doing a PR.
- "Use a recording rule" is often the right answer for derived-metric
  requests; promanomaly emits raw anomaly scores and lets the wider
  Prometheus ecosystem compose on top.
- Forecasting requests belong in [promforecast](https://github.com/esops-dev/promforecast).
  promanomaly deliberately stays out of that lane.

## Development

```bash
make dev-up           # docker-compose: VM + node-exporter + Prometheus + detector
make test             # pytest
make lint             # ruff + mypy --strict
make chart-lint       # helm lint + kubeconform
```

## Pull requests

- One logical change per PR.
- Tests for new behaviour. Synthetic-series fixtures live in `detector/tests/`.
- Output metric names *and label sets* are part of the public API. Renames
  and label additions to existing metrics are breaking changes that
  require a deprecation period.
- Detector parameter **default values** are part of the public API.
  Changing a default silently shifts every operator's calibration; treat
  it like a metric rename.
- `mypy --strict` and `ruff` must be clean.
- Adding a detector? Update three places:
  [detector/src/promanomaly/detectors/](detector/src/promanomaly/detectors/),
  `pyproject.toml` entry points, and
  [docs/detectors.md](docs/detectors.md). CI checks all three.
- Adding a Helm value? Regenerate `values.schema.json` (`make chart-schema`).
- Adding or changing a contracted label? Coordinate a PR pair against
  [LABELS_CONTRACT.md](LABELS_CONTRACT.md) here and in promforecast — CI
  diffs the two files and will fail if they drift.

## Commit style

Conventional Commits are encouraged but not enforced. PR titles flow into
release notes; keep them descriptive.

## Code of conduct

Be kind. Assume good faith. Disagreement on design is welcome; personal
attacks are not.

## License

By contributing you agree your contribution is licensed under
[Apache-2.0](LICENSE).
