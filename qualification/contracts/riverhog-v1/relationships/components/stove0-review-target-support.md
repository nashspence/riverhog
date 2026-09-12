# stove0-review-target-support

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative shared review-target support reference for Stove0.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/stove0/targets/review/support` |
| Description source | `reference/stove0/targets/review/support/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/stove0-review-target-support/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-client](riverhog-client.md) | `required` |
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-review-sampler-client](stove0-review-sampler-client.md) | `required` |
| outgoing | `depends-on` | [stove0-review-sampler-protocol](stove0-review-sampler-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-review-target-contracts](stove0-review-target-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-target-support](stove0-target-support.md) | `required` |
| incoming | `depends-on` | [stove0-review-materialize-target](stove0-review-materialize-target.md) | `required` |
| incoming | `depends-on` | [stove0-review-rclone-effect-target](stove0-review-rclone-effect-target.md) | `required` |
