# https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance-windows-contracts:https-nashspence-github-io-riverhog-v1-pr-780129ba3c:680fb2d9ea -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1windows-backup-stream-info.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/extension-contract/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json` — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/schemas/windows-backup-stream-info.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |
| value | schema-value | `extension_owned` | maximum=None, reason=independently-versioned-extension-authority |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `ea_flags` | no | integer |  |
| `need_ea` | no | boolean |  |
| `stream_attribute_names` | yes | array |  |
| `stream_attributes` | yes | integer |  |
| `stream_id` | yes | integer |  |
| `stream_size` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5749ab8c4944e69d57543fc32f3be4ed3f02eb66c903884166f368a98a0e4b44 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "ea_flags": {
      "minimum": 0,
      "type": "integer"
    },
    "need_ea": {
      "type": "boolean"
    },
    "stream_attribute_names": {
      "items": {
        "minLength": 1,
        "type": "string"
      },
      "type": "array",
      "uniqueItems": true
    },
    "stream_attributes": {
      "minimum": 0,
      "type": "integer"
    },
    "stream_id": {
      "minimum": 0,
      "type": "integer"
    },
    "stream_size": {
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "stream_id",
    "stream_attributes",
    "stream_attribute_names",
    "stream_size"
  ],
  "type": "object"
}
```
