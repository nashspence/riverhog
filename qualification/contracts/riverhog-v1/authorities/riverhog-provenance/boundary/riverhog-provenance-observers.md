# riverhog.provenance-observers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance:riverhog-provenance-observers:ec9ab0b122 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance` |
| Interface | `boundary` |
| Family | `entry-point-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `group` | "riverhog.provenance-observers" |
| `owner` | "riverhog-provenance" |
| `owner_constant` | "PROVENANCE_OBSERVER_ENTRY_POINT_GROUP" |
| `owner_path` | "packages/riverhog-provenance" |
| `providers` | items=additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` |

## Governing policies

- `boundary/frozen-authority/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

### Machine authority

- `/boundaries/entry_point_extensions/3`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f3d605d20af9e4414e252363e434d4c804835100ca9a69af7d5ac9b9bafa061 -->

```json
{
  "group": "riverhog.provenance-observers",
  "owner": "riverhog-provenance",
  "owner_constant": "PROVENANCE_OBSERVER_ENTRY_POINT_GROUP",
  "owner_path": "packages/riverhog-provenance",
  "providers": [
    {
      "distribution": "riverhog-provenance-linux-observer",
      "name": "riverhog-linux",
      "value": "riverhog_provenance_linux_observer:OBSERVER_BINDING"
    },
    {
      "distribution": "riverhog-provenance-macos-observer",
      "name": "riverhog-macos",
      "value": "riverhog_provenance_macos_observer:OBSERVER_BINDING"
    },
    {
      "distribution": "riverhog-provenance-windows-observer",
      "name": "riverhog-windows",
      "value": "riverhog_provenance_windows_observer:OBSERVER_BINDING"
    }
  ]
}
```
