# stove0-nvenc-av1-opus-review-sampler

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative NVENC AV1 and Opus review-sampler reference for Stove0.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/stove0/targets/nvenc-av1-opus/review-sampler` |
| Description source | `reference/stove0/targets/nvenc-av1-opus/review-sampler/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/stove0-nvenc-av1-opus-review-sampler/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-media-archive-target-contracts](stove0-media-archive-target-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-review-sampler-protocol](stove0-review-sampler-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-review-sampler-support](stove0-review-sampler-support.md) | `required` |
| outgoing | `depends-on` | [stove0-review-target-contracts](stove0-review-target-contracts.md) | `required` |
| outgoing | `implements-protocol` | `stove0-review-sampler` | `` |
| outgoing | `packaged-in` | `stove0-nvenc-av1-opus-target` | `` |
