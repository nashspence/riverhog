# riverhog-provenance-contracts

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Canonical Riverhog provenance identity and reference contracts.

| Boundary field | Value |
|---|---|
| Release role | `reusable_library` |
| Source path | `packages/riverhog-provenance-contracts` |
| Description source | `packages/riverhog-provenance-contracts/pyproject.toml#/project/description` |
| Owned contract elements | 3 |

[Open exact owned authority](../../authorities/riverhog-provenance-contracts/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `owns-extension-point` | `riverhog.provenance-contracts` | `` |
| incoming | `depends-on` | [riverhog-client](riverhog-client.md) | `required` |
| incoming | `depends-on` | [riverhog-protocol](riverhog-protocol.md) | `required` |
| incoming | `depends-on` | [riverhog-provenance](riverhog-provenance.md) | `required` |
| incoming | `depends-on` | [riverhog-provenance-linux-contracts](riverhog-provenance-linux-contracts.md) | `required` |
| incoming | `depends-on` | [riverhog-provenance-macos-contracts](riverhog-provenance-macos-contracts.md) | `required` |
| incoming | `depends-on` | [riverhog-provenance-windows-contracts](riverhog-provenance-windows-contracts.md) | `required` |
| incoming | `depends-on` | [riverhog-server](riverhog-server.md) | `required` |
