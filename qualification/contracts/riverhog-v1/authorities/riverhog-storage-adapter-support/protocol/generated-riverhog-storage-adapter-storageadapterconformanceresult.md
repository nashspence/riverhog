# generated:riverhog-storage-adapter: StorageAdapterConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-storag-b0b8ffd825:e78895edd0 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/StorageAdapterConformanceResult`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: StorageAdapterConformanceResult
- `description`: Stable positive evidence returned after the complete check set passes.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `checks` | yes | array |  |
| `coverage` | no | string |  |
| `descriptor` | yes | #/$defs/AdapterDescriptor |  |
| `format` | no | string |  |
| `protocol` | no | string |  |
| `status` | no | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `AdapterDescriptor` | object |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af805afd4d0dffb329ba08dde971222ad008eab2aec7afc4abc0303dd1956caf -->

```json
{
  "$defs": {
    "AdapterDescriptor": {
      "additionalProperties": false,
      "properties": {
        "implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Implementation Id",
          "type": "string"
        },
        "implementation_version": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Implementation Version",
          "type": "string"
        },
        "maximum_segment_bytes": {
          "anyOf": [
            {
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Maximum Segment Bytes"
        },
        "maximum_segment_count": {
          "anyOf": [
            {
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Maximum Segment Count"
        },
        "minimum_nonfinal_segment_bytes": {
          "minimum": 1,
          "title": "Minimum Nonfinal Segment Bytes",
          "type": "integer"
        },
        "protocol": {
          "const": "riverhog-storage-adapter/v1",
          "default": "riverhog-storage-adapter/v1",
          "title": "Protocol",
          "type": "string"
        },
        "read_mode": {
          "enum": [
            "immediate",
            "restore_required"
          ],
          "title": "Read Mode",
          "type": "string"
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "read_mode",
        "minimum_nonfinal_segment_bytes"
      ],
      "title": "AdapterDescriptor",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Stable positive evidence returned after the complete check set passes.",
  "properties": {
    "checks": {
      "items": {
        "type": "string"
      },
      "title": "Checks",
      "type": "array"
    },
    "coverage": {
      "const": "complete",
      "default": "complete",
      "title": "Coverage",
      "type": "string"
    },
    "descriptor": {
      "$ref": "#/$defs/AdapterDescriptor"
    },
    "format": {
      "const": "riverhog-storage-adapter-conformance-result/v1",
      "default": "riverhog-storage-adapter-conformance-result/v1",
      "title": "Format",
      "type": "string"
    },
    "protocol": {
      "const": "riverhog-storage-adapter/v1",
      "default": "riverhog-storage-adapter/v1",
      "title": "Protocol",
      "type": "string"
    },
    "status": {
      "const": "conformant",
      "default": "conformant",
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "descriptor",
    "checks"
  ],
  "title": "StorageAdapterConformanceResult",
  "type": "object"
}
```
