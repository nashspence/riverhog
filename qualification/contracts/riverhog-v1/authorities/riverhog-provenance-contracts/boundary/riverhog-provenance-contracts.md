# riverhog.provenance-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-contracts:riverhog-provenance-contracts:5350bd8bcc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-contracts` |
| Interface | `boundary` |
| Family | `entry-point-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `group` | "riverhog.provenance-contracts" |
| `owner` | "riverhog-provenance-contracts" |
| `owner_constant` | "PROVENANCE_CONTRACT_ENTRY_POINT_GROUP" |
| `owner_path` | "packages/riverhog-provenance-contracts" |
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

- `/boundaries/entry_point_extensions/2`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95c7cde4ca6777b48f9b3333bd7539e7b96cc1fdd0e18aae6ebbefae97dcb7e6 -->

```json
{
  "group": "riverhog.provenance-contracts",
  "owner": "riverhog-provenance-contracts",
  "owner_constant": "PROVENANCE_CONTRACT_ENTRY_POINT_GROUP",
  "owner_path": "packages/riverhog-provenance-contracts",
  "providers": [
    {
      "distribution": "riverhog-provenance-linux-contracts",
      "name": "riverhog-linux",
      "value": "riverhog_provenance_linux_contracts:CONTRACT_BINDING"
    },
    {
      "distribution": "riverhog-provenance-macos-contracts",
      "name": "riverhog-macos",
      "value": "riverhog_provenance_macos_contracts:CONTRACT_BINDING"
    },
    {
      "distribution": "riverhog-provenance-windows-contracts",
      "name": "riverhog-windows",
      "value": "riverhog_provenance_windows_contracts:CONTRACT_BINDING"
    }
  ]
}
```
