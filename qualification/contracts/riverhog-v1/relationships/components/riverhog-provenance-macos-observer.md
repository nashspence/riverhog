# riverhog-provenance-macos-observer

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative macOS filesystem-observer reference for Riverhog provenance.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/riverhog/provenance/observers/macos` |
| Description source | `reference/riverhog/provenance/observers/macos/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/riverhog-provenance-macos-observer/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-provenance](riverhog-provenance.md) | `required` |
| outgoing | `depends-on` | [riverhog-provenance-macos-contracts](riverhog-provenance-macos-contracts.md) | `required` |
| outgoing | `implements-extension-point` | `riverhog.provenance-observers` | `riverhog-macos` |
