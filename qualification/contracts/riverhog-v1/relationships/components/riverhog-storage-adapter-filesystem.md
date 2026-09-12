# riverhog-storage-adapter-filesystem

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative Linux filesystem storage reference for Riverhog.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/riverhog/storage/filesystem` |
| Description source | `reference/riverhog/storage/filesystem/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/riverhog-storage-adapter-filesystem/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-storage-adapter-asgi-support](riverhog-storage-adapter-asgi-support.md) | `required` |
| outgoing | `depends-on` | [riverhog-storage-adapter-protocol](riverhog-storage-adapter-protocol.md) | `required` |
| outgoing | `depends-on` | [time-formats](time-formats.md) | `required` |
| outgoing | `implements-protocol` | `riverhog-storage-adapter` | `` |
| outgoing | `packaged-in` | `riverhog-storage-adapter-filesystem` | `` |
