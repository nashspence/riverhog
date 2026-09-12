# riverhog.provenance-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-contracts:riverhog-provenance-contracts:5350bd8bcc -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-contracts` |
| Interface | `boundary` |
| Family | `entry-point-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/entry_point_extensions/2`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract

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
