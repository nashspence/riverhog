# stove0-target-protocol

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Dependency-light public contracts for external stove0 targets.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `reference/stove0/packages/target-protocol` |
| Description source | `reference/stove0/packages/target-protocol/pyproject.toml#/project/description` |
| Owned contract elements | 3 |

[Open exact owned authority](../../authorities/stove0-target-protocol/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| outgoing | `owns-protocol` | [stove0-target](../extensions/index.md#node-process-protocol-stove0-target) | `` |
| incoming | `depends-on` | [stove0-media-archive-target-contracts](stove0-media-archive-target-contracts.md) | `required` |
| incoming | `depends-on` | [stove0-media-archive-target-support](stove0-media-archive-target-support.md) | `required` |
| incoming | `depends-on` | [stove0-operator-contracts](stove0-operator-contracts.md) | `required` |
| incoming | `depends-on` | [stove0-recipe-config](stove0-recipe-config.md) | `required` |
| incoming | `depends-on` | [stove0-review-target-contracts](stove0-review-target-contracts.md) | `required` |
| incoming | `depends-on` | [stove0-server](stove0-server.md) | `required` |
| incoming | `depends-on` | [stove0-target-client](stove0-target-client.md) | `required` |
| incoming | `depends-on` | [stove0-target-support](stove0-target-support.md) | `required` |
