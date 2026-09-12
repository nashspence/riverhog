# stove0-target-client

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Narrow HTTP client for Stove0 transform targets.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `reference/stove0/packages/target-client` |
| Description source | `reference/stove0/packages/target-client/pyproject.toml#/project/description` |
| Owned contract elements | 2 |

[Open exact owned authority](../../authorities/stove0-target-client/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-target-protocol](stove0-target-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-server](stove0-server.md) | `required` |
| incoming | `depends-on` | [stove0-target-support](stove0-target-support.md) | `required` |
