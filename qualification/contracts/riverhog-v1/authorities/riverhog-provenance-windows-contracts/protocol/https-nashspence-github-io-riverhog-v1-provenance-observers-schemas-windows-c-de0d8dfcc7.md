# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-de0d8dfcc7:68551531bd -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-compression-state.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-compression-state.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `chunk_shift` | yes | integer |  |
| `cluster_shift` | yes | integer |  |
| `compressed_size` | yes | integer |  |
| `compression_format` | yes | integer |  |
| `compression_format_name` | yes | string |  |
| `compression_unit_shift` | yes | integer |  |
| `file_attribute_compressed` | yes | boolean |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 136807e5ab3720e22873df3b3b79f28d20fb61114088a774f4238bb55903c169 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "chunk_shift": {
      "minimum": 0,
      "type": "integer"
    },
    "cluster_shift": {
      "minimum": 0,
      "type": "integer"
    },
    "compressed_size": {
      "minimum": 0,
      "type": "integer"
    },
    "compression_format": {
      "minimum": 0,
      "type": "integer"
    },
    "compression_format_name": {
      "minLength": 1,
      "type": "string"
    },
    "compression_unit_shift": {
      "minimum": 0,
      "type": "integer"
    },
    "file_attribute_compressed": {
      "type": "boolean"
    }
  },
  "required": [
    "file_attribute_compressed",
    "compressed_size",
    "compression_format",
    "compression_format_name",
    "compression_unit_shift",
    "chunk_shift",
    "cluster_shift"
  ],
  "type": "object"
}
```
