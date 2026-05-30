# Configuration Schema Stability

Starting with **v1.0**, promanomaly’s configuration schema is stable. This page explains exactly what that promise means for operators.

## Versioned Surfaces

| Surface                  | Identifier                          | Current value |
|--------------------------|-------------------------------------|---------------|
| Config API               | `apiVersion:` (in YAML)             | `promanomaly.io/v1` |
| Helm values schema       | `$id` in `values.schema.json`       | `https://promanomaly.io/schemas/v1/values.schema.json` |

A breaking change bumps both versions together.

## What v1 Guarantees

**Breaking changes** (require a major version bump + deprecation period):
- Removing or renaming config fields
- Changing default values of fields or detector parameters
- Adding labels to existing output metrics
- Renaming output metrics or labels
- Tightening validation rules

**Non-breaking changes**:
- Adding new config fields or detector parameters (with sensible defaults)
- Adding new output metrics
- Adding **opt-in** labels (only emitted when the feature is explicitly enabled)
- Adding new detectors
- Loosening validation rules

**Rule of thumb**: If your existing config still produces the same output metrics with the same labels and the same values after an upgrade, the change is non-breaking.

## Deprecation Policy

- Deprecated features keep working for **at least one full minor release**.
- The detector emits clear warnings at startup (structured log + `DeprecationWarning`).
- Helm charts show a NOTES warning during install.
- Every deprecation is listed in `CHANGELOG.md`.

## Legacy `v1alpha1` Alias

```yaml
apiVersion: promanomaly.io/v1alpha1   # still works (emits warning)
apiVersion: promanomaly.io/v1         # recommended
```

The alias is kept for backward compatibility and will be removed after v2.0.

## Helm Chart Schema

The JSON Schema `$id` is stable for the entire v1.x line. Any `# yaml-language-server: $schema=...` pins you have in your `values.yaml` files remain valid across all patch and minor releases.

## Coordination with promforecast

promforecast follows the exact same stability rules. Shared concepts (`id`, `group`, and other label contracts) are kept in sync via `LABELS_CONTRACT.md`.

## Automated Config Migration

`promanomaly migrate-config` upgrades config files across `apiVersion` changes mechanically. Supported migrations:

| From | To | Changes |
|------|----|---------|
| `promanomaly.io/v1alpha1` | `promanomaly.io/v1` | `apiVersion` rewrite only (byte-identical schema) |

Use `--dry-run` to preview the diff before writing. The migration is idempotent and never changes detection semantics: a migrated config produces identical output metrics to the original. See [operations.md](operations.md#config-schema-migration) for usage.

---

This is the complete, operator-friendly reference. Upgrading inside v1.x never requires editing your config files.