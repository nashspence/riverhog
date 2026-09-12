# riverhog-provenance

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Portable Riverhog v1 per-file provenance journals and validation.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `packages/riverhog-provenance` |
| Description source | `packages/riverhog-provenance/pyproject.toml#/project/description` |
| Owned contract elements | 10 |

[Open exact owned authority](../../authorities/riverhog-provenance/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-provenance-contracts](riverhog-provenance-contracts.md) | `required` |
| outgoing | `owns-extension-point` | `riverhog.provenance-observers` | `` |
| incoming | `depends-on` | [piggity](piggity.md) | `required` |
| incoming | `depends-on` | [riverhog-ftp-adapter](riverhog-ftp-adapter.md) | `required` |
| incoming | `depends-on` | [riverhog-provenance-linux-observer](riverhog-provenance-linux-observer.md) | `required` |
| incoming | `depends-on` | [riverhog-provenance-macos-observer](riverhog-provenance-macos-observer.md) | `required` |
| incoming | `depends-on` | [riverhog-provenance-windows-observer](riverhog-provenance-windows-observer.md) | `required` |
| incoming | `depends-on` | [riverhog-recover](riverhog-recover.md) | `required` |
| incoming | `depends-on` | [riverhog-server](riverhog-server.md) | `required` |
