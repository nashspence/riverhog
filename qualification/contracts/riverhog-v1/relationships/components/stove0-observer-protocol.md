# stove0-observer-protocol

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Dependency-light public contracts for external stove0 content observers.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `reference/stove0/packages/observer-protocol` |
| Description source | `reference/stove0/packages/observer-protocol/pyproject.toml#/project/description` |
| Owned contract elements | 3 |

[Open exact owned authority](../../authorities/stove0-observer-protocol/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| outgoing | `owns-protocol` | `stove0-observer` | `` |
| incoming | `depends-on` | [stove0-exiftool-observer](stove0-exiftool-observer.md) | `required` |
| incoming | `depends-on` | [stove0-ffprobe-sampling-observer](stove0-ffprobe-sampling-observer.md) | `required` |
| incoming | `depends-on` | [stove0-media-archive-target-support](stove0-media-archive-target-support.md) | `required` |
| incoming | `depends-on` | [stove0-media-metadata-observer-contracts](stove0-media-metadata-observer-contracts.md) | `required` |
| incoming | `depends-on` | [stove0-media-sampling-observer-contracts](stove0-media-sampling-observer-contracts.md) | `required` |
| incoming | `depends-on` | [stove0-observer-client](stove0-observer-client.md) | `required` |
| incoming | `depends-on` | [stove0-observer-support](stove0-observer-support.md) | `required` |
| incoming | `depends-on` | [stove0-operator-contracts](stove0-operator-contracts.md) | `required` |
| incoming | `depends-on` | [stove0-server](stove0-server.md) | `required` |
