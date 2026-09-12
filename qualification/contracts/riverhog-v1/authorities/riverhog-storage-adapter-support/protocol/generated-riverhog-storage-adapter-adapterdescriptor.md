# generated:riverhog-storage-adapter: AdapterDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-adapterdescriptor:fd5ddc3b83 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: AdapterDescriptor
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| `implementation_version` | yes | type="string"; minLength=1; maxLength=120 |  |
| `maximum_segment_bytes` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |
| `maximum_segment_count` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |
| `minimum_nonfinal_segment_bytes` | yes | type="integer"; minimum=1 |  |
| `protocol` | no | type="string"; const="riverhog-storage-adapter/v1" |  |
| `read_mode` | yes | type="string"; enum=["immediate","restore_required"] |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |

## Governing policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/AdapterDescriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cee17550dd897297e60b7829813a6b432932a360b38b800c81292b1cbdda754 -->

```json
{
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
```
