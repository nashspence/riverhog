# generated:riverhog-storage-adapter: ObjectReadReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-objectreadreceipt:e833f18a4c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ObjectReadReceipt`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: ObjectReadReceipt
- `description`: Adapter-observed identity and range for one single-pass read.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `object` | yes | #/$defs/ObjectLocator |  |
| `offset` | yes | integer |  |
| `read_bytes` | yes | integer |  |
| `total_bytes` | yes | integer |  |

### Definitions

| Definition | Shape |
|---|---|
| `ObjectLocator` | object |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c00ab742b44d1b708bac1d252dd1dd6fb4882bdec441eb330f63746d635820ce -->

```json
{
  "$defs": {
    "ObjectLocator": {
      "additionalProperties": false,
      "properties": {
        "object_path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Object Path",
          "type": "string"
        },
        "revision": {
          "anyOf": [
            {
              "maxLength": 2000,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Revision"
        }
      },
      "required": [
        "object_path"
      ],
      "title": "ObjectLocator",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Adapter-observed identity and range for one single-pass read.",
  "properties": {
    "object": {
      "$ref": "#/$defs/ObjectLocator"
    },
    "offset": {
      "minimum": 0,
      "title": "Offset",
      "type": "integer"
    },
    "read_bytes": {
      "minimum": 0,
      "title": "Read Bytes",
      "type": "integer"
    },
    "total_bytes": {
      "minimum": 0,
      "title": "Total Bytes",
      "type": "integer"
    }
  },
  "required": [
    "object",
    "total_bytes",
    "offset",
    "read_bytes"
  ],
  "title": "ObjectReadReceipt",
  "type": "object"
}
```
