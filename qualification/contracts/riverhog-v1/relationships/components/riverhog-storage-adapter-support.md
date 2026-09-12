# riverhog-storage-adapter-support

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

HTTP binding and conformance support for Riverhog storage adapters.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `packages/riverhog-storage-adapter-support` |
| Description source | `packages/riverhog-storage-adapter-support/pyproject.toml#/project/description` |
| Owned contract elements | 26 |

[Open exact owned authority](../../authorities/riverhog-storage-adapter-support/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `binds-protocol` | [riverhog-storage-adapter](../extensions/index.md#node-process-protocol-riverhog-storage-adapter) | `http` |
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-storage-adapter-protocol](riverhog-storage-adapter-protocol.md) | `required` |
| incoming | `depends-on` | [riverhog-server](riverhog-server.md) | `required` |
| incoming | `depends-on` | [riverhog-storage-adapter-asgi-support](riverhog-storage-adapter-asgi-support.md) | `required` |
