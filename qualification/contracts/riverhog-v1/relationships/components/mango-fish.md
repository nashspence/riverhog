# mango-fish

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative CloudEvents reference application for Riverhog.

| Boundary field | Value |
|---|---|
| Release role | `reference_application` |
| Source path | `reference/riverhog/applications/mango-fish` |
| Description source | `reference/riverhog/applications/mango-fish/pyproject.toml#/project/description` |
| Owned contract elements | 7 |

[Open exact owned authority](../../authorities/mango-fish/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [lifecycle-events](lifecycle-events.md) | `required` |
| outgoing | `depends-on` | [state-schema](state-schema.md) | `required` |
| outgoing | `packaged-in` | `mango-fish` | `` |
