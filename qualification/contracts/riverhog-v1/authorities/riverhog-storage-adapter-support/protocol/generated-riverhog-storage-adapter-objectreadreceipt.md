# generated:riverhog-storage-adapter: ObjectReadReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-objectreadreceipt:e833f18a4c -->

Adapter-observed identity and range for one single-pass read.

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: ObjectReadReceipt
- `description`: Adapter-observed identity and range for one single-pass read.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `object` | yes | #/$defs/ObjectLocator |  |
| `offset` | yes | type="integer"; minimum=0 |  |
| `read_bytes` | yes | type="integer"; minimum=0 |  |
| `total_bytes` | yes | type="integer"; minimum=0 |  |

### Definitions

| Definition | Shape |
|---|---|
| `ObjectLocator` | type="object"; fields=`object_path`, `revision`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Governing policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ObjectReadReceipt`

### Exact owned JSON

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
