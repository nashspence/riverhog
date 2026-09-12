# riverhog.provenance-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-contracts:riverhog-provenance-contracts:5350bd8bcc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [boundary](index.md) |
| Family | [entry-point-extensions](index.md#f-6802f8cd9c8c) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-777805b6721d"></a>
| Field | Shape |
|---|---|
| <a id="s-17972993fd16"></a>`group` | "riverhog.provenance-contracts" |
| <a id="s-1f37834fbeb3"></a>`owner` | "riverhog-provenance-contracts" |
| <a id="s-59f11e077e57"></a>`owner_constant` | "PROVENANCE_CONTRACT_ENTRY_POINT_GROUP" |
| <a id="s-f8106519fd5f"></a>`owner_path` | "packages/riverhog-provenance-contracts" |
| <a id="s-036e69a94566"></a>`providers` | items=additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` |

## Governing policies

- <a id="pa-97c4704f90dd"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

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
