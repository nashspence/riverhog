# generated:riverhog-storage-adapter: WriteSegmentListRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writes-3419a4c482:875a665498 -->

Request one bounded page from an exact accepted-segment view.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-03eab200df"></a>

- <a id="s-102093f05e"></a>`type`: `"object"`
- <a id="s-6a64d139a3"></a>`additionalProperties`: `false`
- <a id="s-ddcc4da5bb"></a>`description`: `"Request one bounded page from an exact accepted-segment view."`
- <a id="s-fb75016dff"></a>`required`: `["session"]`
- <a id="s-acf29da67e"></a>`title`: `"WriteSegmentListRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f1d3323083"></a>`after_number` | no | [NonnegativeDecimal](#s-f75d48c36d); default=0; x-riverhog-extent={"policy":"segmented_no_total_max","reason":"write-segment-history-bounded-traversal"} |  |
| <a id="s-86d5f555d2"></a>`maximum_items` | no | type="integer"; minimum=1; maximum=128; default=128; title="Maximum Items" |  |
| <a id="s-9142d95f27"></a>`session` | yes | [WriteSession](#s-524b3d9072) |  |
| <a id="s-6b5c1418bd"></a>`traversal_token` | no | anyOf=[(type="string"; maxLength=4000; minLength=1); (type="null")]; default=null; title="Traversal Token" |  |

### Definitions

- [NonnegativeDecimal](#s-f75d48c36d)
- [PositiveDecimal](#s-24ddcd1ce2)
- [WriteSession](#s-524b3d9072)

### <a id="s-f75d48c36d"></a>definition `NonnegativeDecimal`

- <a id="s-8d31f07532"></a>`type`: `"string"`
- <a id="s-57493cfd19"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

### <a id="s-24ddcd1ce2"></a>definition `PositiveDecimal`

- <a id="s-abca34bf23"></a>`type`: `"string"`
- <a id="s-23f052799d"></a>`pattern`: `"^[1-9][0-9]*(?![\\s\\S])"`

### <a id="s-524b3d9072"></a>definition `WriteSession`

- <a id="s-bccf3a8405"></a>`type`: `"object"`
- <a id="s-d80d3fc7bc"></a>`additionalProperties`: `false`
- <a id="s-41af59998a"></a>`required`: `["object_path","expected_bytes","write_token"]`
- <a id="s-7aa0e05e25"></a>`title`: `"WriteSession"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ec7a1bfb7a"></a>`expected_bytes` | yes | [PositiveDecimal](#s-24ddcd1ce2) | Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal. |
| <a id="s-6859981f1c"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-ca2109b609"></a>`write_token` | yes | type="string"; maxLength=4000; minLength=1; title="Write Token" | Opaque adapter-owned persistable continuation handle. For the same configured adapter it remains replayable across client, transport, Riverhog, and adapter process restarts until completion, explicit abort, or caller-authorized incomplete-write reclamation makes the write terminal. |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field maximum_items](#s-86d5f555d2) | `value · schema-value · contract_max` | maximum=128 |
| <a id="s-eb7d397f6b"></a>[field traversal_token · string value](#s-6b5c1418bd) | `length · characters · contract_max` | maximum=4000 |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-5b1d75141c"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-55b4e26e23"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSegmentListRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 298689d5b60eb07a5344896513365973a1f5b6d9989c3eaeaf992351a569c82a -->

```json
{
  "$defs": {
    "NonnegativeDecimal": {
      "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
      "type": "string"
    },
    "PositiveDecimal": {
      "pattern": "^[1-9][0-9]*(?![\\s\\S])",
      "type": "string"
    },
    "WriteSession": {
      "additionalProperties": false,
      "properties": {
        "expected_bytes": {
          "$ref": "#/$defs/PositiveDecimal",
          "description": "Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal."
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
      "$ref": "#/$defs/NonnegativeDecimal",
      "default": 0,
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

</details>
