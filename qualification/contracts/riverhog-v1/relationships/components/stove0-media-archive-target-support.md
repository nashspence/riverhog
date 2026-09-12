# stove0-media-archive-target-support

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative projection support for Stove0 media-archive references.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/stove0/targets/media-archive/support` |
| Description source | `reference/stove0/targets/media-archive/support/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/stove0-media-archive-target-support/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-media-archive-target-contracts](stove0-media-archive-target-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-media-metadata-observer-contracts](stove0-media-metadata-observer-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-observer-protocol](stove0-observer-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-target-protocol](stove0-target-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-nvenc-av1-opus-target](stove0-nvenc-av1-opus-target.md) | `required` |
| incoming | `depends-on` | [stove0-opus-target](stove0-opus-target.md) | `required` |
