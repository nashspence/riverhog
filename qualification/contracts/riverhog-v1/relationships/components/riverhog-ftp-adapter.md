# riverhog-ftp-adapter

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative FTP ingress reference for Riverhog.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/riverhog/ingress/ftp` |
| Description source | `reference/riverhog/ingress/ftp/pyproject.toml#/project/description` |
| Owned contract elements | 26 |

[Open exact owned authority](../../authorities/riverhog-ftp-adapter/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [http-api-contracts](http-api-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-client](riverhog-client.md) | `required` |
| outgoing | `depends-on` | [riverhog-ftp-adapter-api-client](riverhog-ftp-adapter-api-client.md) | `required` |
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [riverhog-provenance](riverhog-provenance.md) | `required` |
| outgoing | `packaged-in` | [riverhog-ftp-adapter](../runtime-images/index.md#node-image-runtime-riverhog-ftp-adapter) | `` |
