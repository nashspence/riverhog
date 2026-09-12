# riverhog-storage-adapter-asgi-support

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Authenticated ASGI shell for independently scoped Riverhog storage adapters.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `packages/riverhog-storage-adapter-asgi-support` |
| Description source | `packages/riverhog-storage-adapter-asgi-support/pyproject.toml#/project/description` |
| Owned contract elements | 2 |

[Open exact owned authority](../../authorities/riverhog-storage-adapter-asgi-support/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-storage-adapter-protocol](riverhog-storage-adapter-protocol.md) | `required` |
| outgoing | `depends-on` | [riverhog-storage-adapter-support](riverhog-storage-adapter-support.md) | `required` |
| incoming | `depends-on` | [riverhog-storage-adapter-aws](riverhog-storage-adapter-aws.md) | `required` |
| incoming | `depends-on` | [riverhog-storage-adapter-backblaze](riverhog-storage-adapter-backblaze.md) | `required` |
| incoming | `depends-on` | [riverhog-storage-adapter-filesystem](riverhog-storage-adapter-filesystem.md) | `required` |
