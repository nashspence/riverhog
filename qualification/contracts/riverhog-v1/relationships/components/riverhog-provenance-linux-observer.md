# riverhog-provenance-linux-observer

[Atlas](../../index.md) · [Relationships](../index.md) · [Components](index.md)

Optional nonnormative Linux filesystem-observer reference for Riverhog provenance.

| Boundary field | Value |
|---|---|
| Release role | `reference_component` |
| Source path | `reference/riverhog/provenance/observers/linux` |
| Description source | `reference/riverhog/provenance/observers/linux/pyproject.toml#/project/description` |
| Owned contract elements | 1 |

[Open exact owned authority](../../authorities/riverhog-provenance-linux-observer/index.md)

## Typed relationships

| Direction | Relationship | Counterparty | Scope or binding |
|---|---|---|---|
| outgoing | `depends-on` | [riverhog-provenance](riverhog-provenance.md) | `required` |
| outgoing | `depends-on` | [riverhog-provenance-linux-contracts](riverhog-provenance-linux-contracts.md) | `required` |
| outgoing | `implements-extension-point` | `riverhog.provenance-observers` | `riverhog-linux` |
| outgoing | `packaged-in` | `riverhog-ftp-adapter` | `` |
