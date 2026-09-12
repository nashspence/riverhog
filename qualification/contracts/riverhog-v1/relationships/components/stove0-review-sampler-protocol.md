# stove0-review-sampler-protocol

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative sampler-protocol reference for the Stove0 review target.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/stove0/targets/review/sampler/protocol` |
| Description source | `reference/stove0/targets/review/sampler/protocol/pyproject.toml#/project/description` |
| Owned contract elements | 2 |

[Open exact owned authority](../../authorities/stove0-review-sampler-protocol/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| outgoing | `owns-protocol` | [stove0-review-sampler](../extensions/index.md#node-process-protocol-stove0-review-sampler) | `` |
| incoming | `depends-on` | [stove0-nvenc-av1-opus-review-sampler](stove0-nvenc-av1-opus-review-sampler.md) | `required` |
| incoming | `depends-on` | [stove0-opus-review-sampler](stove0-opus-review-sampler.md) | `required` |
| incoming | `depends-on` | [stove0-review-sampler-client](stove0-review-sampler-client.md) | `required` |
| incoming | `depends-on` | [stove0-review-sampler-support](stove0-review-sampler-support.md) | `required` |
| incoming | `depends-on` | [stove0-review-target-support](stove0-review-target-support.md) | `required` |
