# stove0-server

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative content-opaque transformation reference application for Riverhog.

| Boundary field | Value |
|---|---|
| Release role | `reference_application` |
| Source path | `reference/stove0/application/server` |
| Description source | `reference/stove0/application/server/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/stove0-server/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-client](riverhog-client.md) | `required` |
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [state-schema](state-schema.md) | `required` |
| outgoing | `depends-on` | [stove0-observer-client](stove0-observer-client.md) | `required` |
| outgoing | `depends-on` | [stove0-observer-protocol](stove0-observer-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-operator-contracts](stove0-operator-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-recipe-config](stove0-recipe-config.md) | `required` |
| outgoing | `depends-on` | [stove0-target-client](stove0-target-client.md) | `required` |
| outgoing | `depends-on` | [stove0-target-protocol](stove0-target-protocol.md) | `required` |
| outgoing | `depends-on` | [time-formats](time-formats.md) | `required` |
| outgoing | `packaged-in` | [stove0](../runtime-images/index.md#node-image-runtime-stove0) | `` |
