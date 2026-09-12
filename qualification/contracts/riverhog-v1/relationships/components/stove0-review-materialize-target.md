# stove0-review-materialize-target

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative review materialization target reference for Stove0.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/stove0/targets/review/materialize-target` |
| Description source | `reference/stove0/targets/review/materialize-target/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/stove0-review-materialize-target/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-client](riverhog-client.md) | `required` |
| outgoing | `depends-on` | [stove0-review-target-contracts](stove0-review-target-contracts.md) | `required` |
| outgoing | `depends-on` | [stove0-review-target-support](stove0-review-target-support.md) | `required` |
| outgoing | `depends-on` | [stove0-target-support](stove0-target-support.md) | `required` |
| outgoing | `implements-protocol` | `stove0-target` | `` |
| outgoing | `packaged-in` | `stove0-review-materialize-target` | `` |
