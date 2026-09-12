# riverhog-provenance-macos-contracts

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative macOS observation-contract reference for Riverhog provenance.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/riverhog/provenance/contracts/macos` |
| Description source | `reference/riverhog/provenance/contracts/macos/pyproject.toml#/project/description` |
| Owned contract elements | 5 |

[Open exact owned authority](../../authorities/riverhog-provenance-macos-contracts/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-provenance-contracts](riverhog-provenance-contracts.md) | `required` |
| outgoing | `implements-extension-point` | `riverhog.provenance-contracts` | `riverhog-macos` |
| incoming | `depends-on` | [riverhog-provenance-macos-observer](riverhog-provenance-macos-observer.md) | `required` |
