# riverhog-storage-adapter-protocol

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Provider-neutral opaque-object capability contracts for Riverhog storage adapters.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `packages/riverhog-storage-adapter-protocol` |
| Description source | `packages/riverhog-storage-adapter-protocol/pyproject.toml#/project/description` |
| Owned contract elements | 3 |

[Open exact owned authority](../../authorities/riverhog-storage-adapter-protocol/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [time-formats](time-formats.md) | `required` |
| outgoing | `owns-protocol` | [riverhog-storage-adapter](../extensions/index.md#node-process-protocol-riverhog-storage-adapter) | `` |
| incoming | `depends-on` | [riverhog-server](riverhog-server.md) | `required` |
| incoming | `depends-on` | [riverhog-storage-adapter-asgi-support](riverhog-storage-adapter-asgi-support.md) | `required` |
| incoming | `depends-on` | [riverhog-storage-adapter-aws](riverhog-storage-adapter-aws.md) | `required` |
| incoming | `depends-on` | [riverhog-storage-adapter-filesystem](riverhog-storage-adapter-filesystem.md) | `required` |
| incoming | `depends-on` | [riverhog-storage-adapter-s3-support](riverhog-storage-adapter-s3-support.md) | `required` |
| incoming | `depends-on` | [riverhog-storage-adapter-support](riverhog-storage-adapter-support.md) | `required` |
