# generated:riverhog-storage-adapter: WriteSegmentPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writesegmentpage:04eb8b9379 -->

One bounded page under an adapter-owned immutable traversal view.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-d39c66e138"></a>

- <a id="s-eebb216bdf"></a>`type`: `"object"`
- <a id="s-5a0fc5c6e4"></a>`additionalProperties`: `false`
- <a id="s-bccfafd22d"></a>`description`: `"One bounded page under an adapter-owned immutable traversal view."`
- <a id="s-9d672c5f5e"></a>`required`: `["session","traversal_token"]`
- <a id="s-3251df53fd"></a>`title`: `"WriteSegmentPage"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-05723f37a4"></a>`completion` | no | anyOf=[([WriteCompletionAuthority](#s-21da49f925)); (type="null")]; default=null |  |
| <a id="s-2130080be0"></a>`next_after_number` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null; title="Next After Number" |  |
| <a id="s-33643dd5a1"></a>`segments` | no | type="array"; default=[]; items=([WriteSegmentReceipt](#s-49182d9498)); maxItems=128; title="Segments"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"exact-adapter-write-traversal","reason":"bounded-storage-write-segment-page"} |  |
| <a id="s-2b84ac917f"></a>`session` | yes | [WriteSession](#s-182505f3d5) |  |
| <a id="s-401c234753"></a>`traversal_token` | yes | type="string"; maxLength=4000; minLength=1; title="Traversal Token" |  |

### Definitions

- [WriteCompletionAuthority](#s-21da49f925)
- [WriteSegmentReceipt](#s-49182d9498)
- [WriteSession](#s-182505f3d5)

### <a id="s-21da49f925"></a>definition `WriteCompletionAuthority`

- <a id="s-5187ac4808"></a>`type`: `"object"`
- <a id="s-21193e2351"></a>`additionalProperties`: `false`
- <a id="s-a335faf8e1"></a>`description`: `"Adapter-issued terminal authority for one exact active-write state.\n\nConsumers echo the opaque token unchanged. It is neither a credential nor a\nbearer capability; completion remains independently authorized. Once an exact\nimmutable object is published, its completed-object identity supersedes this\ntransport authority for terminal reconciliation."`
- <a id="s-b39f82fd05"></a>`required`: `["segment_count","stored_bytes","authority_token"]`
- <a id="s-a21e438197"></a>`title`: `"WriteCompletionAuthority"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-be34d2262f"></a>`authority_token` | yes | type="string"; maxLength=4000; minLength=1; title="Authority Token" | Bounded opaque adapter-issued authority for the exact accepted state of an active write. The token grants no authority and must be echoed unchanged. |
| <a id="s-10adf909eb"></a>`segment_count` | yes | type="integer"; minimum=0; title="Segment Count" |  |
| <a id="s-347c213eaf"></a>`stored_bytes` | yes | type="integer"; minimum=0; title="Stored Bytes" |  |

### <a id="s-49182d9498"></a>definition `WriteSegmentReceipt`

- <a id="s-10f4eeb6f9"></a>`type`: `"object"`
- <a id="s-650eab54e1"></a>`additionalProperties`: `false`
- <a id="s-325e4e8529"></a>`required`: `["number","segment_token","stored_bytes"]`
- <a id="s-2e21b73a54"></a>`title`: `"WriteSegmentReceipt"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4cd5e84a2c"></a>`number` | yes | type="integer"; minimum=1; title="Number" |  |
| <a id="s-61c3694c07"></a>`segment_token` | yes | type="string"; maxLength=4000; minLength=1; title="Segment Token" |  |
| <a id="s-888b147fe3"></a>`stored_bytes` | yes | type="integer"; minimum=1; title="Stored Bytes" |  |
| <a id="s-bf436c18ac"></a>`stored_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Stored Sha256" |  |

### <a id="s-182505f3d5"></a>definition `WriteSession`

- <a id="s-c5f3b5c39f"></a>`type`: `"object"`
- <a id="s-c16ac49bbd"></a>`additionalProperties`: `false`
- <a id="s-ea97a3d165"></a>`required`: `["object_path","expected_bytes","write_token"]`
- <a id="s-e091763de4"></a>`title`: `"WriteSession"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-191777666f"></a>`expected_bytes` | yes | type="integer"; minimum=1; title="Expected Bytes" | Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal. |
| <a id="s-d8564aa07c"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-059cc0748b"></a>`write_token` | yes | type="string"; maxLength=4000; minLength=1; title="Write Token" | Opaque adapter-owned persistable continuation handle. For the same configured adapter it remains replayable across client, transport, Riverhog, and adapter process restarts until completion, explicit abort, or caller-authorized incomplete-write reclamation makes the write terminal. |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=128; minimum=null; progression={"progression":"exact-adapter-write-traversal"}; reason="bounded-storage-write-segment-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field segments](#s-33643dd5a1) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field traversal_token](#s-401c234753) | `length · characters · contract_max` | maximum=4000; minimum=1; reason="schema-maximum" |
| [definition WriteSegmentReceipt · field segment_token](#s-61c3694c07) | `length · characters · contract_max` | maximum=4000; minimum=1; reason="schema-maximum" |
| <a id="s-0833484d84"></a>[definition WriteSegmentReceipt · field stored_sha256 · string value](#s-bf436c18ac) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594).

Exact evidence groups for this contract element:

- [riverhog-storage-write-segment-progression/v1](../../../evidence/qualifications/riverhog-storage-write-segment-progression-v1/index.md)

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-15a40b5664"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-3b45f16905"></a>[extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)
- <a id="pa-2afec23897"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSegmentPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
