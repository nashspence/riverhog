# riverhog-storage-adapter-aws

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative AWS storage reference for Riverhog.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/riverhog/storage/aws` |
| Description source | `reference/riverhog/storage/aws/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/riverhog-storage-adapter-aws/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-storage-adapter-asgi-support](riverhog-storage-adapter-asgi-support.md) | `required` |
| outgoing | `depends-on` | [riverhog-storage-adapter-protocol](riverhog-storage-adapter-protocol.md) | `required` |
| outgoing | `depends-on` | [riverhog-storage-adapter-s3-support](riverhog-storage-adapter-s3-support.md) | `required` |
| outgoing | `depends-on` | [time-formats](time-formats.md) | `required` |
| outgoing | `implements-protocol` | `riverhog-storage-adapter` | `` |
| outgoing | `packaged-in` | `riverhog-storage-adapter-aws` | `` |
