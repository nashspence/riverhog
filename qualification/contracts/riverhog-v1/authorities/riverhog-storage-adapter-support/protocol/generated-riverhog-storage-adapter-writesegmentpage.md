# generated:riverhog-storage-adapter: WriteSegmentPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writesegmentpage:f462fcb7a3 -->

One bounded page under an adapter-owned immutable traversal view.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-d39c66e13820"></a>
- <a id="s-3251df53fd41"></a>`title`: WriteSegmentPage
- <a id="s-bccfafd22d64"></a>`description`: One bounded page under an adapter-owned immutable traversal view.
- <a id="s-eebb216bdf7e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-05723f37a4da"></a>`completion` | no | anyOf=#/$defs/WriteCompletionAuthority \| type="null" |  |
| <a id="s-2130080be089"></a>`next_after_number` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |
| <a id="s-33643dd5a1cc"></a>`segments` | no | type="array"; maxItems=128; items=(#/$defs/WriteSegmentReceipt); additional keys=`x-riverhog-extent` |  |
| <a id="s-2b84ac917f78"></a>`session` | yes | #/$defs/WriteSession |  |
| <a id="s-401c234753c5"></a>`traversal_token` | yes | type="string"; minLength=1; maxLength=4000 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-21da49f925e7"></a>`WriteCompletionAuthority` | type="object"; fields=`authority_token`, `segment_count`, `stored_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-49182d949805"></a>`WriteSegmentReceipt` | type="object"; fields=`number`, `segment_token`, `stored_bytes`, `stored_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-182505f3d5fc"></a>`WriteSession` | type="object"; fields=`expected_bytes`, `object_path`, `write_token`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)

Shared facts for every subject below: maximum=128; minimum=null; progression={"progression":"exact-adapter-write-traversal"}; reason="bounded-storage-write-segment-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field segments](#s-33643dd5a1cc) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field traversal_token](#s-401c234753c5) | `length · characters · contract_max` | maximum=4000; minimum=1; reason="schema-maximum" |
| <a id="s-61c3694c0779"></a>definition WriteSegmentReceipt · field segment_token | `length · characters · contract_max` | maximum=4000; minimum=1; reason="schema-maximum" |
| <a id="s-0833484d84d0"></a>definition WriteSegmentReceipt · field stored_sha256 · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-3f7c311fbdb2"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-beb565a54a65"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)
- <a id="pa-c9fc8289af98"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471a9) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSegmentPage`

### Exact owned JSON

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
