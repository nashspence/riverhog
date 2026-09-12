# stove0-observer-client

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Narrow HTTP client for Stove0 content observers.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `reference/stove0/packages/observer-client` |
| Description source | `reference/stove0/packages/observer-client/pyproject.toml#/project/description` |
| Owned contract elements | 3 |

[Open exact owned authority](../../authorities/stove0-observer-client/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-observer-protocol](stove0-observer-protocol.md) | `required` |
| outgoing | `owns-extension-point` | `stove0.observer-semantic-validators` | `` |
| incoming | `depends-on` | [stove0-observer-support](stove0-observer-support.md) | `required` |
| incoming | `depends-on` | [stove0-server](stove0-server.md) | `required` |
