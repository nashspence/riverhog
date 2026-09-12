# config-validation

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Strict YAML and JSON Schema configuration validation.

| Boundary field | Value |
|---|---|
| Release role | `internal_build_unit` |
| Source path | `packages/config-validation` |
| Description source | `packages/config-validation/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/config-validation/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| incoming | `depends-on` | [gogurt](gogurt.md) | `required` |
| incoming | `depends-on` | [gogurt-core](gogurt-core.md) | `required` |
| incoming | `depends-on` | [gogurt-listener-runtime](gogurt-listener-runtime.md) | `required` |
| incoming | `depends-on` | [gogurt-path-volume-support](gogurt-path-volume-support.md) | `required` |
| incoming | `depends-on` | [stove0-recipe-config](stove0-recipe-config.md) | `required` |
