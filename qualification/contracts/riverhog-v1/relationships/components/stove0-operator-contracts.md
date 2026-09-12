# stove0-operator-contracts

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Canonical public state contracts for the Stove0 v1 operator surface.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `reference/stove0/packages/operator-contracts` |
| Description source | `reference/stove0/packages/operator-contracts/pyproject.toml#/project/description` |
| Owned contract elements | 2 |

[Open exact owned authority](../../authorities/stove0-operator-contracts/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [lifecycle-events](lifecycle-events.md) | `required` |
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-observer-protocol](stove0-observer-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-recipe-config](stove0-recipe-config.md) | `required` |
| outgoing | `depends-on` | [stove0-target-protocol](stove0-target-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-api-client](stove0-api-client.md) | `required` |
| incoming | `depends-on` | [stove0-server](stove0-server.md) | `required` |
