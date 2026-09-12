# riverhog-client

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Typed generic Riverhog client and capability-scoped collection-processing runtime.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `packages/riverhog-client` |
| Description source | `packages/riverhog-client/pyproject.toml#/project/description` |
| Owned contract elements | 3 |

[Open exact owned authority](../../authorities/riverhog-client/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-application-access](riverhog-application-access.md) | `required` |
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [riverhog-provenance-contracts](riverhog-provenance-contracts.md) | `required` |
| incoming | `depends-on` | [piggity](piggity.md) | `required` |
| incoming | `depends-on` | [riverhog-ftp-adapter](riverhog-ftp-adapter.md) | `required` |
| incoming | `depends-on` | [stove0-nvenc-av1-opus-target](stove0-nvenc-av1-opus-target.md) | `required` |
| incoming | `depends-on` | [stove0-observer-support](stove0-observer-support.md) | `required` |
| incoming | `depends-on` | [stove0-opus-target](stove0-opus-target.md) | `required` |
| incoming | `depends-on` | [stove0-review-materialize-target](stove0-review-materialize-target.md) | `required` |
| incoming | `depends-on` | [stove0-review-rclone-effect-target](stove0-review-rclone-effect-target.md) | `required` |
| incoming | `depends-on` | [stove0-review-target-support](stove0-review-target-support.md) | `required` |
| incoming | `depends-on` | [stove0-server](stove0-server.md) | `required` |
| incoming | `depends-on` | [stove0-target-support](stove0-target-support.md) | `required` |
