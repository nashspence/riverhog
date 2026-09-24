# generated:riverhog-storage-adapter: SmallObjectWriteRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-smallo-12ddfe9106:4380c4e923 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-25fab1a1b0"></a>

- <a id="s-376ce5c3c7"></a>`type`: `"object"`
- <a id="s-1a59983e51"></a>`additionalProperties`: `false`
- <a id="s-d142c2462b"></a>`required`: `["object_path","content_type","required_identity_assertions","placement_policy","mode","stored_bytes","stored_sha256"]`
- <a id="s-2040f98675"></a>`title`: `"SmallObjectWriteRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-245dd83a60"></a>`content_type` | yes | type="string"; maxLength=255; minLength=1; title="Content Type" |  |
| <a id="s-694f5617c3"></a>`expected_current_stored_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Expected Current Stored Sha256" |  |
| <a id="s-cddc05ccf5"></a>`mode` | yes | type="string"; enum=["create_only","replace_current"]; title="Mode" |  |
| <a id="s-9555273565"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-2c7543503f"></a>`placement_policy` | yes | type="string"; enum=["archive_default","immediate_default"]; title="Placement Policy" | Select an adapter-configured placement default. This does not establish archive membership or read readiness. |
| <a id="s-a8fdc8c0a1"></a>`required_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; title="Required Identity Assertions"; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |
| <a id="s-831f49620d"></a>`stored_bytes` | yes | [NonnegativeDecimal](#s-726aeae300) |  |
| <a id="s-d8fcaa470d"></a>`stored_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Stored Sha256" |  |

### Definitions

- [NonnegativeDecimal](#s-726aeae300)

### <a id="s-726aeae300"></a>definition `NonnegativeDecimal`

- <a id="s-067d65f35a"></a>`type`: `"string"`
- <a id="s-55fd34b226"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field content_type](#s-245dd83a60) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| <a id="s-889c05b3f3"></a>[field expected_current_stored_sha256 · string value](#s-694f5617c3) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field object_path](#s-9555273565) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field required_identity_assertions](#s-a8fdc8c0a1) | `encoded-size · bytes · contract_max` | maximum=16384; reason="bounded-object-identity-assertion-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field required_identity_assertions](#s-a8fdc8c0a1) | `cardinality · entries · contract_max` | maximum=64; reason="bounded-object-identity-assertion-envelope" |
| [field stored_sha256](#s-d8fcaa470d) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-9f0acd55ac"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-6fd6655b86"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/SmallObjectWriteRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f180bab05433fbe1b47a138e524e161f999b49e7fd77955e1edb954bd8b86f36 -->

```json
{
  "$defs": {
    "NonnegativeDecimal": {
      "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "properties": {
    "content_type": {
      "maxLength": 255,
      "minLength": 1,
      "title": "Content Type",
      "type": "string"
    },
    "expected_current_stored_sha256": {
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
      "title": "Expected Current Stored Sha256"
    },
    "mode": {
      "enum": [
        "create_only",
        "replace_current"
      ],
      "title": "Mode",
      "type": "string"
    },
    "object_path": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Object Path",
      "type": "string"
    },
    "placement_policy": {
      "description": "Select an adapter-configured placement default. This does not establish archive membership or read readiness.",
      "enum": [
        "archive_default",
        "immediate_default"
      ],
      "title": "Placement Policy",
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
    "stored_bytes": {
      "$ref": "#/$defs/NonnegativeDecimal"
    },
    "stored_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Stored Sha256",
      "type": "string"
    }
  },
  "required": [
    "object_path",
    "content_type",
    "required_identity_assertions",
    "placement_policy",
    "mode",
    "stored_bytes",
    "stored_sha256"
  ],
  "title": "SmallObjectWriteRequest",
  "type": "object"
}
```

</details>
