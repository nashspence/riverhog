# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-6df2b70cc6:a12e386383 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-integrity-info.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-integrity-info.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `checksum_algorithm` | yes | integer |  |
| `checksum_chunk_size` | yes | integer |  |
| `cluster_size` | yes | integer |  |
| `flags` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 986c12768cb14d0aceae4c926c74f7230aac4e51cd9cbb0c04e6453b35a2aaad -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "checksum_algorithm": {
      "minimum": 0,
      "type": "integer"
    },
    "checksum_chunk_size": {
      "minimum": 0,
      "type": "integer"
    },
    "cluster_size": {
      "minimum": 0,
      "type": "integer"
    },
    "flags": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "checksum_algorithm",
    "flags",
    "checksum_chunk_size",
    "cluster_size"
  ],
  "type": "object"
}
```
