# generated:riverhog-storage-adapter: DeleteObjectRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-delete-877fb953a2:b72fa48ed8 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/DeleteObjectRequest`

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
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=2000, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: DeleteObjectRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `expected_current_stored_sha256` | no | object (3 fields) |  |
| `mode` | yes | string |  |
| `object` | yes | #/$defs/ObjectLocator |  |

### Definitions

| Definition | Shape |
|---|---|
| `ObjectLocator` | object |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 46f15404c356f50e3a381b04f209d8f64b9114cce82ef9e8c42967f06b59fad8 -->

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
  "properties": {
    "expected_current_stored_sha256": {
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
      "title": "Expected Current Stored Sha256"
    },
    "mode": {
      "enum": [
        "current",
        "exact_revision",
        "all_versions"
      ],
      "title": "Mode",
      "type": "string"
    },
    "object": {
      "$ref": "#/$defs/ObjectLocator"
    }
  },
  "required": [
    "object",
    "mode"
  ],
  "title": "DeleteObjectRequest",
  "type": "object"
}
```
