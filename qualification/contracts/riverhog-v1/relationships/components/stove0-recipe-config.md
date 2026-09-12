# stove0-recipe-config

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Portable deployment-owned Stove0 recipe catalog contracts and validation.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `reference/stove0/packages/recipe-config` |
| Description source | `reference/stove0/packages/recipe-config/pyproject.toml#/project/description` |
| Owned contract elements | 2 |

[Open exact owned authority](../../authorities/stove0-recipe-config/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [config-validation](config-validation.md) | `required` |
| outgoing | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-target-protocol](stove0-target-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-client](stove0-client.md) | `required` |
| incoming | `depends-on` | [stove0-operator-contracts](stove0-operator-contracts.md) | `required` |
| incoming | `depends-on` | [stove0-server](stove0-server.md) | `required` |
