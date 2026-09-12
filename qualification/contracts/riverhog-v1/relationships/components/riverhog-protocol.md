# riverhog-protocol

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Canonical Riverhog wire and identity contracts.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `packages/riverhog-protocol` |
| Description source | `packages/riverhog-protocol/pyproject.toml#/project/description` |
| Owned contract elements | 3 |

[Open exact owned authority](../../authorities/riverhog-protocol/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [lifecycle-events](lifecycle-events.md) | `required` |
| outgoing | `depends-on` | [riverhog-provenance-contracts](riverhog-provenance-contracts.md) | `required` |
| incoming | `depends-on` | [piggity](piggity.md) | `required` |
| incoming | `depends-on` | [riverhog-application-access](riverhog-application-access.md) | `required` |
| incoming | `depends-on` | [riverhog-client](riverhog-client.md) | `required` |
| incoming | `depends-on` | [riverhog-ftp-adapter](riverhog-ftp-adapter.md) | `required` |
| incoming | `depends-on` | [riverhog-recover](riverhog-recover.md) | `required` |
| incoming | `depends-on` | [riverhog-server](riverhog-server.md) | `required` |
| incoming | `depends-on` | [stove0-media-archive-target-support](stove0-media-archive-target-support.md) | `required` |
| incoming | `depends-on` | [stove0-nvenc-av1-opus-target](stove0-nvenc-av1-opus-target.md) | `required` |
| incoming | `depends-on` | [stove0-operator-contracts](stove0-operator-contracts.md) | `required` |
| incoming | `depends-on` | [stove0-opus-target](stove0-opus-target.md) | `required` |
| incoming | `depends-on` | [stove0-protocol](stove0-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-review-rclone-effect-target](stove0-review-rclone-effect-target.md) | `required` |
| incoming | `depends-on` | [stove0-review-sampler-protocol](stove0-review-sampler-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-review-target-support](stove0-review-target-support.md) | `required` |
| incoming | `depends-on` | [stove0-server](stove0-server.md) | `required` |
| incoming | `depends-on` | [stove0-target-protocol](stove0-target-protocol.md) | `required` |
| incoming | `depends-on` | [stove0-target-support](stove0-target-support.md) | `required` |
