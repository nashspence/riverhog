# stove0-media-metadata-observer-contracts

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative media-metadata contract reference for Stove0 observers.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/stove0/observers/contracts/media-metadata` |
| Description source | `reference/stove0/observers/contracts/media-metadata/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/stove0-media-metadata-observer-contracts/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [stove0-observer-protocol](stove0-observer-protocol.md) | `required` |
| outgoing | `implements-extension-point` | `stove0.observer-semantic-validators` | `media-metadata` |
| incoming | `depends-on` | [stove0-exiftool-observer](stove0-exiftool-observer.md) | `required` |
| incoming | `depends-on` | [stove0-media-archive-target-support](stove0-media-archive-target-support.md) | `required` |
