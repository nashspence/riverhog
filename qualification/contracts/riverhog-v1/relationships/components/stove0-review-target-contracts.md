# stove0-review-target-contracts

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative review contract reference for Stove0 targets.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/stove0/targets/review/contracts` |
| Description source | `reference/stove0/targets/review/contracts/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/stove0-review-target-contracts/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-target-protocol](stove0-target-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-nvenc-av1-opus-review-sampler](stove0-nvenc-av1-opus-review-sampler.md) | `required` |
| incoming | `depends-on` | [stove0-opus-review-sampler](stove0-opus-review-sampler.md) | `required` |
| incoming | `depends-on` | [stove0-review-materialize-target](stove0-review-materialize-target.md) | `required` |
| incoming | `depends-on` | [stove0-review-planning](stove0-review-planning.md) | `required` |
| incoming | `depends-on` | [stove0-review-rclone-effect-target](stove0-review-rclone-effect-target.md) | `required` |
| incoming | `depends-on` | [stove0-review-target-support](stove0-review-target-support.md) | `required` |
