# riverhog-ftp-adapter-api-client

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative client for the Riverhog FTP ingress reference.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/riverhog/ingress/ftp-api-client` |
| Description source | `reference/riverhog/ingress/ftp-api-client/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/riverhog-ftp-adapter-api-client/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| incoming | `depends-on` | [riverhog-ftp-adapter](riverhog-ftp-adapter.md) | `required` |
