# riverhog-provenance-windows-observer

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative Windows filesystem-observer reference for Riverhog provenance.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/riverhog/provenance/observers/windows` |
| Description source | `reference/riverhog/provenance/observers/windows/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/riverhog-provenance-windows-observer/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-provenance](riverhog-provenance.md) | `required` |
| outgoing | `depends-on` | [riverhog-provenance-windows-contracts](riverhog-provenance-windows-contracts.md) | `required` |
| outgoing | `implements-extension-point` | `riverhog.provenance-observers` | `riverhog-windows` |
