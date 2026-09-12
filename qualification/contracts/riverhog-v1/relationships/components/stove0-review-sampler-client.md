# stove0-review-sampler-client

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative sampler-client reference for the Stove0 review target.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/stove0/targets/review/sampler/client` |
| Description source | `reference/stove0/targets/review/sampler/client/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/stove0-review-sampler-client/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-review-sampler-protocol](stove0-review-sampler-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-review-sampler-support](stove0-review-sampler-support.md) | `required` |
| incoming | `depends-on` | [stove0-review-target-support](stove0-review-target-support.md) | `required` |
