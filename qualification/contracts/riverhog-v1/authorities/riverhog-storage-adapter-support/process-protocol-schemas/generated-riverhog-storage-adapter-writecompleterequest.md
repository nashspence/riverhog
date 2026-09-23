# generated:riverhog-storage-adapter: WriteCompleteRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writec-6bf48f348a:49609401bd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-ae1ba73678"></a>

- <a id="s-9b8bcb2574"></a>`type`: `"object"`
- <a id="s-a008380d81"></a>`additionalProperties`: `false`
- <a id="s-a8f407501e"></a>`required`: `["session","completion","expected_bytes","expected_content_type","required_identity_assertions","expected_placement"]`
- <a id="s-c628be94cf"></a>`title`: `"WriteCompleteRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6c1d725696"></a>`completion` | yes | [WriteCompletionAuthority](#s-2aa29473ed) |  |
| <a id="s-3dd31e3915"></a>`expected_bytes` | yes | [PositiveDecimal](#s-037927588e) |  |
| <a id="s-918194abd0"></a>`expected_content_type` | yes | type="string"; maxLength=255; minLength=1; title="Expected Content Type" |  |
| <a id="s-41c9c882c5"></a>`expected_placement` | yes | type="string"; enum=["archive","immediate"]; title="Expected Placement" |  |
| <a id="s-3fe7699e11"></a>`required_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; title="Required Identity Assertions"; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |
| <a id="s-b80752731f"></a>`session` | yes | [WriteSession](#s-43526cbe13) |  |

### Definitions

- [NonnegativeDecimal](#s-0541f4a6f1)
- [PositiveDecimal](#s-037927588e)
- [WriteCompletionAuthority](#s-2aa29473ed)
- [WriteSession](#s-43526cbe13)

### <a id="s-0541f4a6f1"></a>definition `NonnegativeDecimal`

- <a id="s-cf6c772cb1"></a>`type`: `"string"`
- <a id="s-f7647e678f"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

### <a id="s-037927588e"></a>definition `PositiveDecimal`

- <a id="s-8f0cfb79a1"></a>`type`: `"string"`
- <a id="s-426acd6167"></a>`pattern`: `"^[1-9][0-9]*(?![\\s\\S])"`

### <a id="s-2aa29473ed"></a>definition `WriteCompletionAuthority`

- <a id="s-3c3fa2eba9"></a>`type`: `"object"`
- <a id="s-c1b61d9dac"></a>`additionalProperties`: `false`
- <a id="s-b59a631e9d"></a>`description`: `"Adapter-issued terminal authority for one exact active-write state.\n\nConsumers echo the opaque token unchanged. It is neither a credential nor a\nbearer capability; completion remains independently authorized. Once an exact\nimmutable object is published, its completed-object identity supersedes this\ntransport authority for terminal reconciliation."`
- <a id="s-1e75fe9c08"></a>`required`: `["segment_count","stored_bytes","authority_token"]`
- <a id="s-fd5367c2a1"></a>`title`: `"WriteCompletionAuthority"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-924d57268f"></a>`authority_token` | yes | type="string"; maxLength=4000; minLength=1; title="Authority Token" | Bounded opaque adapter-issued authority for the exact accepted state of an active write. The token grants no authority and must be echoed unchanged. |
| <a id="s-f2d5b6894e"></a>`segment_count` | yes | [NonnegativeDecimal](#s-0541f4a6f1) |  |
| <a id="s-212652088c"></a>`stored_bytes` | yes | [NonnegativeDecimal](#s-0541f4a6f1) |  |

### <a id="s-43526cbe13"></a>definition `WriteSession`

- <a id="s-b180909d70"></a>`type`: `"object"`
- <a id="s-25059f0349"></a>`additionalProperties`: `false`
- <a id="s-993867265f"></a>`required`: `["object_path","expected_bytes","write_token"]`
- <a id="s-7562c3205c"></a>`title`: `"WriteSession"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2975343229"></a>`expected_bytes` | yes | [PositiveDecimal](#s-037927588e) | Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal. |
| <a id="s-ff6e987a77"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-751c151e29"></a>`write_token` | yes | type="string"; maxLength=4000; minLength=1; title="Write Token" | Opaque adapter-owned persistable continuation handle. For the same configured adapter it remains replayable across client, transport, Riverhog, and adapter process restarts until completion, explicit abort, or caller-authorized incomplete-write reclamation makes the write terminal. |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field expected_content_type](#s-918194abd0) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field required_identity_assertions](#s-3fe7699e11) | `encoded-size · bytes · contract_max` | maximum=16384; reason="bounded-object-identity-assertion-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field required_identity_assertions](#s-3fe7699e11) | `cardinality · entries · contract_max` | maximum=64; reason="bounded-object-identity-assertion-envelope" |
| [definition WriteCompletionAuthority · field authority_token](#s-924d57268f) | `length · characters · contract_max` | maximum=4000; minimum=1; reason="schema-maximum" |
| [definition WriteSession · field object_path](#s-ff6e987a77) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition WriteSession · field write_token](#s-751c151e29) | `length · characters · contract_max` | maximum=4000; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-9c85c56100"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-a450b22168"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteCompleteRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f386e6cd14bead8fb90a8ed7e4c982afbe74605b3497340bc801520171d97998 -->

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
          "$ref": "#/$defs/NonnegativeDecimal"
        },
        "stored_bytes": {
          "$ref": "#/$defs/NonnegativeDecimal"
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
  "properties": {
    "completion": {
      "$ref": "#/$defs/WriteCompletionAuthority"
    },
    "expected_bytes": {
      "$ref": "#/$defs/PositiveDecimal"
    },
    "expected_content_type": {
      "maxLength": 255,
      "minLength": 1,
      "title": "Expected Content Type",
      "type": "string"
    },
    "expected_placement": {
      "enum": [
        "archive",
        "immediate"
      ],
      "title": "Expected Placement",
      "type": "string"
    },
    "required_identity_assertions": {
      "additionalProperties": {
        "type": "string"
      },
      "description": "Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions.",
      "maxProperties": 64,
      "title": "Required Identity Assertions",
      "type": "object",
      "x-riverhog-encoded-bytes-max": 16384,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-object-identity-assertion-envelope"
      }
    },
    "session": {
      "$ref": "#/$defs/WriteSession"
    }
  },
  "required": [
    "session",
    "completion",
    "expected_bytes",
    "expected_content_type",
    "required_identity_assertions",
    "expected_placement"
  ],
  "title": "WriteCompleteRequest",
  "type": "object"
}
```

</details>
