# generated:riverhog-storage-adapter: WriteSegmentReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writes-6ba9873e65:ff59f32b06 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSegmentReceipt`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=4000, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: WriteSegmentReceipt
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `number` | yes | integer |  |
| `segment_token` | yes | string |  |
| `stored_bytes` | yes | integer |  |
| `stored_sha256` | no | object (3 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 78f82d84ff68316a37a5d4d95d212d082ea0eada74506b6745aee67978f169da -->

```json
{
  "additionalProperties": false,
  "properties": {
    "number": {
      "minimum": 1,
      "title": "Number",
      "type": "integer"
    },
    "segment_token": {
      "maxLength": 4000,
      "minLength": 1,
      "title": "Segment Token",
      "type": "string"
    },
    "stored_bytes": {
      "minimum": 1,
      "title": "Stored Bytes",
      "type": "integer"
    },
    "stored_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Stored Sha256"
    }
  },
  "required": [
    "number",
    "segment_token",
    "stored_bytes"
  ],
  "title": "WriteSegmentReceipt",
  "type": "object"
}
```
