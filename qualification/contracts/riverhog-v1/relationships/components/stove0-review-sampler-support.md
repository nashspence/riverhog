# stove0-review-sampler-support

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative sampler support for Stove0 review references.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/stove0/targets/review/sampler/support` |
| Description source | `reference/stove0/targets/review/sampler/support/pyproject.toml#/project/description` |
| Owned contract elements | 7 |

[Open exact owned authority](../../authorities/stove0-review-sampler-support/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `binds-protocol` | [stove0-review-sampler](../extensions/index.md#node-process-protocol-stove0-review-sampler) | `http` |
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-review-sampler-client](stove0-review-sampler-client.md) | `required` |
| outgoing | `depends-on` | [stove0-review-sampler-protocol](stove0-review-sampler-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-nvenc-av1-opus-review-sampler](stove0-nvenc-av1-opus-review-sampler.md) | `required` |
| incoming | `depends-on` | [stove0-opus-review-sampler](stove0-opus-review-sampler.md) | `required` |
