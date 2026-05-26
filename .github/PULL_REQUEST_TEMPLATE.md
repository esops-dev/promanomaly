<!--
Thanks for the contribution. Fill in the sections below; remove any that
don't apply. Keep the description focused on what changed and why — the
diff already shows how.
-->

## Summary

<!-- One or two sentences. What problem does this solve? -->

## Linked issue

<!-- e.g. Closes #42. Open an issue first for non-trivial changes. -->

## Type of change

- [ ] Bug fix
- [ ] New feature / detector / config knob
- [ ] Refactor (no user-visible change)
- [ ] Documentation
- [ ] Helm chart change
- [ ] Build / CI / release plumbing
- [ ] Dependency change

## Test plan

<!--
How did you verify this works? Tick what applies and add specifics.
-->

- [ ] `make ci` is green locally
- [ ] Added or updated unit tests
- [ ] Verified against a local `make dev-up` stack

## Public API impact

<!--
Output metric names, label sets, detector parameter defaults, config
keys, exit codes, chart values — these are part of the public API.
-->

- [ ] No public-API change
- [ ] Output metric names or label sets changed (requires deprecation period)
- [ ] Detector parameter default changed (requires deprecation period)
- [ ] Config schema changed (`values.schema.json` regenerated)
- [ ] Helm values changed (documented in chart README)
- [ ] `LABELS_CONTRACT.md` changed (coordinated PR pair against promforecast)

## Documentation

- [ ] Updated [README.md](../README.md)
- [ ] Updated [docs/](../docs/) for the affected feature
- [ ] Updated example configs / alerts under `examples/`
- [ ] No docs change needed (please explain why)

## Dependency changes

<!--
If this PR adds, removes, or upgrades a dependency, justify it briefly.
-->

## Backwards compatibility

<!--
Are output metric names, label sets, detector defaults, config keys, or
chart values changing? Say so explicitly so release notes capture it.
-->

## Anything else

<!-- Screenshots, before/after output, design notes, follow-ups. -->
