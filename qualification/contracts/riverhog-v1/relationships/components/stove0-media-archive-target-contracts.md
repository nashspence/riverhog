# stove0-media-archive-target-contracts

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative media-archive contract reference for Stove0 targets.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/stove0/targets/media-archive/contracts` |
| Description source | `reference/stove0/targets/media-archive/contracts/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/stove0-media-archive-target-contracts/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-target-protocol](stove0-target-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-media-archive-target-support](stove0-media-archive-target-support.md) | `required` |
| incoming | `depends-on` | [stove0-nvenc-av1-opus-review-sampler](stove0-nvenc-av1-opus-review-sampler.md) | `required` |
| incoming | `depends-on` | [stove0-nvenc-av1-opus-target](stove0-nvenc-av1-opus-target.md) | `required` |
| incoming | `depends-on` | [stove0-opus-review-sampler](stove0-opus-review-sampler.md) | `required` |
| incoming | `depends-on` | [stove0-opus-target](stove0-opus-target.md) | `required` |
