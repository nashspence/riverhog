# stove0-ffprobe-sampling-observer

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative FFprobe sampling-observer reference for Stove0.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/stove0/observers/ffprobe-sampling` |
| Description source | `reference/stove0/observers/ffprobe-sampling/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/stove0-ffprobe-sampling-observer/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-media-sampling-observer-contracts](stove0-media-sampling-observer-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-observer-protocol](stove0-observer-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-observer-support](stove0-observer-support.md) | `required` |
| outgoing | `implements-protocol` | `stove0-observer` | `` |
| outgoing | `packaged-in` | `stove0-ffprobe-sampling-observer` | `` |
