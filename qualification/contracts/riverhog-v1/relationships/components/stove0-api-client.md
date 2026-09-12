# stove0-api-client

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Official Python client for the stove0 v1 workflow API.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `reference/stove0/packages/api-client` |
| Description source | `reference/stove0/packages/api-client/pyproject.toml#/project/description` |
| Owned contract elements | 2 |

[Open exact owned authority](../../authorities/stove0-api-client/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-operator-contracts](stove0-operator-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-client](stove0-client.md) | `required` |
