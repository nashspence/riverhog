# generated:riverhog-storage-adapter: WriteSegmentListRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writes-3419a4c482:3017df454c -->

Request one bounded page from an exact accepted-segment view.

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: WriteSegmentListRequest
- `description`: Request one bounded page from an exact accepted-segment view.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `after_number` | no | type="integer"; minimum=0; additional keys=`x-riverhog-extent` |  |
| `maximum_items` | no | type="integer"; minimum=1; maximum=128 |  |
| `session` | yes | #/$defs/WriteSession |  |
| `traversal_token` | no | anyOf=type="string"; minLength=1; maxLength=4000 \| type="null" |  |

### Definitions

| Definition | Shape |
|---|---|
| `WriteSession` | type="object"; fields=`expected_bytes`, `object_path`, `write_token`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| value | schema-value | `contract_max` | maximum=128, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=4000, minimum=1, reason=schema-maximum |

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

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSegmentListRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c655ec3f5640ade7ac2d3b60a3b9bb3de1ecaf39de3d4bcd731afb029a65684f -->

```json
{
  "$defs": {
    "WriteSession": {
      "additionalProperties": false,
      "properties": {
        "expected_bytes": {
          "description": "Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal.",
          "minimum": 1,
          "title": "Expected Bytes",
          "type": "integer"
        },
        "object_path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Object Path",
          "type": "string"
        },
        "write_token": {
          "description": "Opaque adapter-owned persistable continuation handle. For the same configured adapter it remains replayable across client, transport, Riverhog, and adapter process restarts until completion, explicit abort, or caller-authorized incomplete-write reclamation makes the write terminal.",
          "maxLength": 4000,
          "minLength": 1,
          "title": "Write Token",
          "type": "string"
        }
      },
      "required": [
        "object_path",
        "expected_bytes",
        "write_token"
      ],
      "title": "WriteSession",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Request one bounded page from an exact accepted-segment view.",
  "properties": {
    "after_number": {
      "default": 0,
      "minimum": 0,
      "title": "After Number",
      "type": "integer",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "reason": "write-segment-history-bounded-traversal"
      }
    },
    "maximum_items": {
      "default": 128,
      "maximum": 128,
      "minimum": 1,
      "title": "Maximum Items",
      "type": "integer"
    },
    "session": {
      "$ref": "#/$defs/WriteSession"
    },
    "traversal_token": {
      "anyOf": [
        {
          "maxLength": 4000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Traversal Token"
    }
  },
  "required": [
    "session"
  ],
  "title": "WriteSegmentListRequest",
  "type": "object"
}
```
