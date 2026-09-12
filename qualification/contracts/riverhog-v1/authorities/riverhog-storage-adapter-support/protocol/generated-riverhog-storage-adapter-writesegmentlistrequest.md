# generated:riverhog-storage-adapter: WriteSegmentListRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writes-3419a4c482:3017df454c -->

Request one bounded page from an exact accepted-segment view.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-03eab200df"></a>
- <a id="s-acf29da67e"></a>`title`: WriteSegmentListRequest
- <a id="s-ddcc4da5bb"></a>`description`: Request one bounded page from an exact accepted-segment view.
- <a id="s-102093f05e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f1d3323083"></a>`after_number` | no | type="integer"; minimum=0; additional keys=`x-riverhog-extent` |  |
| <a id="s-86d5f555d2"></a>`maximum_items` | no | type="integer"; minimum=1; maximum=128 |  |
| <a id="s-9142d95f27"></a>`session` | yes | #/$defs/WriteSession |  |
| <a id="s-6b5c1418bd"></a>`traversal_token` | no | anyOf=type="string"; minLength=1; maxLength=4000 \| type="null" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-524b3d9072"></a>`WriteSession` | type="object"; fields=`expected_bytes`, `object_path`, `write_token`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field maximum_items](#s-86d5f555d2) | `value · schema-value · contract_max` | maximum=128 |
| <a id="s-eb7d397f6b"></a>[field traversal_token · string value](#s-6b5c1418bd) | `length · characters · contract_max` | maximum=4000 |

## Governing policies

- <a id="pa-ffaae1b1ef"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-50ff663e57"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

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
