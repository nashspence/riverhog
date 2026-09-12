# generated:riverhog-storage-adapter: WriteSegmentPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writesegmentpage:f462fcb7a3 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSegmentPage`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/bounded-segment/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=128, minimum=None, reason=bounded-storage-write-segment-page |
| length | characters | `contract_max` | maximum=4000, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=4000, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: WriteSegmentPage
- `description`: One bounded page under an adapter-owned immutable traversal view.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `completion` | no | object (2 fields) |  |
| `next_after_number` | no | object (3 fields) |  |
| `segments` | no | array |  |
| `session` | yes | #/$defs/WriteSession |  |
| `traversal_token` | yes | string |  |

### Definitions

| Definition | Shape |
|---|---|
| `WriteCompletionAuthority` | object |
| `WriteSegmentReceipt` | object |
| `WriteSession` | object |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d1d971aecf4fda3444d770576aa32bb6527302511e9401a9b027918ec8ae3b38 -->

```json
{
  "$defs": {
    "WriteCompletionAuthority": {
      "additionalProperties": false,
      "description": "Adapter-issued terminal authority for one exact active-write state.\n\nConsumers echo the opaque token unchanged. It is neither a credential nor a\nbearer capability; completion remains independently authorized. Once an exact\nimmutable object is published, its completed-object identity supersedes this\ntransport authority for terminal reconciliation.",
      "properties": {
        "authority_token": {
          "description": "Bounded opaque adapter-issued authority for the exact accepted state of an active write. The token grants no authority and must be echoed unchanged.",
          "maxLength": 4000,
          "minLength": 1,
          "title": "Authority Token",
          "type": "string"
        },
        "segment_count": {
          "minimum": 0,
          "title": "Segment Count",
          "type": "integer"
        },
        "stored_bytes": {
          "minimum": 0,
          "title": "Stored Bytes",
          "type": "integer"
        }
      },
      "required": [
        "segment_count",
        "stored_bytes",
        "authority_token"
      ],
      "title": "WriteCompletionAuthority",
      "type": "object"
    },
    "WriteSegmentReceipt": {
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
    },
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
  "description": "One bounded page under an adapter-owned immutable traversal view.",
  "properties": {
    "completion": {
      "anyOf": [
        {
          "$ref": "#/$defs/WriteCompletionAuthority"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "next_after_number": {
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
      "title": "Next After Number"
    },
    "segments": {
      "default": [],
      "items": {
        "$ref": "#/$defs/WriteSegmentReceipt"
      },
      "maxItems": 128,
      "title": "Segments",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "exact-adapter-write-traversal",
        "reason": "bounded-storage-write-segment-page"
      }
    },
    "session": {
      "$ref": "#/$defs/WriteSession"
    },
    "traversal_token": {
      "maxLength": 4000,
      "minLength": 1,
      "title": "Traversal Token",
      "type": "string"
    }
  },
  "required": [
    "session",
    "traversal_token"
  ],
  "title": "WriteSegmentPage",
  "type": "object"
}
```
