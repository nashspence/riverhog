# stove0-opus-target

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative Opus target reference for Stove0.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/stove0/targets/opus/target` |
| Description source | `reference/stove0/targets/opus/target/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/stove0-opus-target/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-client](riverhog-client.md) | `required` |
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-media-archive-target-contracts](stove0-media-archive-target-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-media-archive-target-support](stove0-media-archive-target-support.md) | `required` |
| outgoing | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-target-support](stove0-target-support.md) | `required` |
| outgoing | `implements-protocol` | [stove0-target](../extensions/index.md#node-process-protocol-stove0-target) | `` |
| outgoing | `packaged-in` | [stove0-opus-target](../runtime-images/index.md#node-image-runtime-stove0-opus-target) | `` |
