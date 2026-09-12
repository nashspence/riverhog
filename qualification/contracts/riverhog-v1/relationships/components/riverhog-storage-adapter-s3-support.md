# riverhog-storage-adapter-s3-support

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative S3 support for Riverhog storage references.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/riverhog/storage/s3-support` |
| Description source | `reference/riverhog/storage/s3-support/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/riverhog-storage-adapter-s3-support/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-storage-adapter-protocol](riverhog-storage-adapter-protocol.md) | `required` |
| outgoing | `depends-on` | [time-formats](time-formats.md) | `required` |
| incoming | `depends-on` | [riverhog-storage-adapter-aws](riverhog-storage-adapter-aws.md) | `required` |
| incoming | `depends-on` | [riverhog-storage-adapter-backblaze](riverhog-storage-adapter-backblaze.md) | `required` |
