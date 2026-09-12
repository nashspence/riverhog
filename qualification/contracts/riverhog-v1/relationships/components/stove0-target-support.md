# stove0-target-support

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Hardware-neutral target protocol, runtime, and conformance support for stove0.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `reference/stove0/packages/target-support` |
| Description source | `reference/stove0/packages/target-support/pyproject.toml#/project/description` |
| Owned contract elements | 11 |

[Open exact owned authority](../../authorities/stove0-target-support/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `binds-protocol` | `stove0-target` | `http` |
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-client](riverhog-client.md) | `required` |
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [stove0-target-client](stove0-target-client.md) | `required` |
| outgoing | `depends-on` | [stove0-target-protocol](stove0-target-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-nvenc-av1-opus-target](stove0-nvenc-av1-opus-target.md) | `required` |
| incoming | `depends-on` | [stove0-opus-target](stove0-opus-target.md) | `required` |
| incoming | `depends-on` | [stove0-review-materialize-target](stove0-review-materialize-target.md) | `required` |
| incoming | `depends-on` | [stove0-review-rclone-effect-target](stove0-review-rclone-effect-target.md) | `required` |
| incoming | `depends-on` | [stove0-review-target-support](stove0-review-target-support.md) | `required` |
