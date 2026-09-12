# riverhog-provenance-windows-contracts

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative Windows observation-contract reference for Riverhog provenance.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/riverhog/provenance/contracts/windows` |
| Description source | `reference/riverhog/provenance/contracts/windows/pyproject.toml#/project/description` |
| Owned contract elements | 11 |

[Open exact owned authority](../../authorities/riverhog-provenance-windows-contracts/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-provenance-contracts](riverhog-provenance-contracts.md) | `required` |
| outgoing | `implements-extension-point` | [riverhog.provenance-contracts](../extensions/index.md#node-extension-point-riverhog-provenance-contracts) | `riverhog-windows` |
| incoming | `depends-on` | [riverhog-provenance-windows-observer](riverhog-provenance-windows-observer.md) | `required` |
