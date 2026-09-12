# riverhog-server

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Encrypted archive management, catalog, and retrieval.

| Boundary field | Value |
|---|---|
| Release role | `deployed_implementation` |
| Source path | `riverhog` |
| Description source | `riverhog/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/riverhog-server/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [lifecycle-events](lifecycle-events.md) | `required` |
| outgoing | `depends-on` | [riverhog-age](riverhog-age.md) | `required` |
| outgoing | `depends-on` | [riverhog-application-access](riverhog-application-access.md) | `required` |
| outgoing | `depends-on` | [riverhog-archive-contracts](riverhog-archive-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [riverhog-provenance](riverhog-provenance.md) | `required` |
| outgoing | `depends-on` | [riverhog-provenance-contracts](riverhog-provenance-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-storage-adapter-protocol](riverhog-storage-adapter-protocol.md) | `required` |
| outgoing | `depends-on` | [riverhog-storage-adapter-support](riverhog-storage-adapter-support.md) | `required` |
| outgoing | `depends-on` | [state-schema](state-schema.md) | `required` |
| outgoing | `depends-on` | [time-formats](time-formats.md) | `required` |
| outgoing | `packaged-in` | [riverhog](../runtime-images/index.md#node-image-runtime-riverhog) | `` |
