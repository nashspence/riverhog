# riverhog-provenance-linux-contracts

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative Linux observation-contract reference for Riverhog provenance.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/riverhog/provenance/contracts/linux` |
| Description source | `reference/riverhog/provenance/contracts/linux/pyproject.toml#/project/description` |
| Owned contract elements | 6 |

[Open exact owned authority](../../authorities/riverhog-provenance-linux-contracts/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-provenance-contracts](riverhog-provenance-contracts.md) | `required` |
| outgoing | `implements-extension-point` | `riverhog.provenance-contracts` | `riverhog-linux` |
| incoming | `depends-on` | [riverhog-provenance-linux-observer](riverhog-provenance-linux-observer.md) | `required` |
