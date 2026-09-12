# riverhog-recover

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative independent recovery reference application for Riverhog archives.

| Boundary field | Value |
|---|---|
| Release role | `reference_application` |
| Source path | `reference/riverhog/recovery` |
| Description source | `reference/riverhog/recovery/pyproject.toml#/project/description` |
| Owned contract elements | 2 |

[Open exact owned authority](../../authorities/riverhog-recover/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-archive-contracts](riverhog-archive-contracts.md) | `required` |
| outgoing | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| outgoing | `depends-on` | [riverhog-provenance](riverhog-provenance.md) | `required` |
| outgoing | `installed-as` | [uv-tool](../installation/index.md#node-installation-uv-tool) | `` |
