# riverhog-application-access

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Public Riverhog application-access contracts and canonical grant grammar.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `packages/riverhog-application-access` |
| Description source | `packages/riverhog-application-access/pyproject.toml#/project/description` |
| Owned contract elements | 2 |

[Open exact owned authority](../../authorities/riverhog-application-access/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| incoming | `depends-on` | [piggity](piggity.md) | `required` |
| incoming | `depends-on` | [riverhog-client](riverhog-client.md) | `required` |
| incoming | `depends-on` | [riverhog-server](riverhog-server.md) | `required` |
