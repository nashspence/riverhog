# generated:stove0-observer: ObserverContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-observer-support:generated-stove0-observer-observercontract:756d416c27 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-aa1a382aed"></a>

- <a id="s-a0fb97db45"></a>`type`: `"object"`
- <a id="s-97192ce111"></a>`additionalProperties`: `false`
- <a id="s-0973fd4f0f"></a>`required`: `["id","options_schema","facts_schema","facts_semantics","contract_sha256"]`
- <a id="s-b62b596dee"></a>`title`: `"ObserverContract"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-85313ce2b5"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-61f5412780"></a>`facts_schema` | yes | [JsonSchemaDocument](#s-481f0f7d22) |  |
| <a id="s-081de5bb8e"></a>`facts_semantics` | yes | [SemanticValidationProfile](#s-e626583caf) |  |
| <a id="s-69e52675a4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-6e47cc2497"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-d234b7bcaf"></a>`options_schema` | yes | [JsonSchemaDocument](#s-481f0f7d22) |  |

### Definitions

- [JsonSchemaDocument](#s-481f0f7d22)
- [JsonValue](#s-9b0b8d807a)
- [SemanticValidationProfile](#s-e626583caf)

### <a id="s-481f0f7d22"></a>definition `JsonSchemaDocument`

- <a id="s-2b8718c4cd"></a>`type`: `"object"`
- <a id="s-afdb0b89b6"></a>`additionalProperties`: `false`
- <a id="s-ed3687a63a"></a>`required`: `["id","sha256","schema"]`
- <a id="s-2f61d00406"></a>`title`: `"JsonSchemaDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-098c49decb"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-72244e6436"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-a7132c206b"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-669b591dce"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-9b0b8d807a)); title="Schema" |  |
| <a id="s-cce2ec7190"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-9b0b8d807a"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-e626583caf"></a>definition `SemanticValidationProfile`

- <a id="s-c2a46a0572"></a>`type`: `"object"`
- <a id="s-21ce615efa"></a>`additionalProperties`: `false`
- <a id="s-1bffe4680b"></a>`required`: `["id","rules","profile_sha256"]`
- <a id="s-1f808d42d0"></a>`title`: `"SemanticValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-10de4b8a41"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Conformance Vectors Sha256" |  |
| <a id="s-7ffd47229c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-999e7ff05c"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-d1644eb1bc"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Rules" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field contract_sha256](#s-85313ce2b5) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field maximum_result_bytes](#s-6e47cc2497) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md)

## Governing policies

- <a id="pa-638a161bb5"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ec3200d398"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-observer](../../../evidence/sources.md#src-dcc0b5485b) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::observer\_schema\_bundle](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObserverContract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 79326d184bed03d400708fc7fca5dca34ea84a3c02cb3e536cd3788f1daf1ff7 -->

```json
{
  "$defs": {
    "JsonSchemaDocument": {
      "additionalProperties": false,
      "properties": {
        "dialect": {
          "const": "https://json-schema.org/draft/2020-12/schema",
          "default": "https://json-schema.org/draft/2020-12/schema",
          "title": "Dialect",
          "type": "string"
        },
        "format_policy": {
          "const": "annotation-only",
          "default": "annotation-only",
          "title": "Format Policy",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "schema": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Schema",
          "type": "object"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "sha256",
        "schema"
      ],
      "title": "JsonSchemaDocument",
      "type": "object"
    },
    "JsonValue": {},
    "SemanticValidationProfile": {
      "additionalProperties": false,
      "properties": {
        "conformance_vectors_sha256": {
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
          "title": "Conformance Vectors Sha256"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Profile Sha256",
          "type": "string"
        },
        "rules": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "minItems": 1,
          "title": "Rules",
          "type": "array"
        }
      },
      "required": [
        "id",
        "rules",
        "profile_sha256"
      ],
      "title": "SemanticValidationProfile",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Contract Sha256",
      "type": "string"
    },
    "facts_schema": {
      "$ref": "#/$defs/JsonSchemaDocument"
    },
    "facts_semantics": {
      "$ref": "#/$defs/SemanticValidationProfile"
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "maximum_result_bytes": {
      "default": 1048576,
      "maximum": 67108864,
      "minimum": 1,
      "title": "Maximum Result Bytes",
      "type": "integer"
    },
    "options_schema": {
      "$ref": "#/$defs/JsonSchemaDocument"
    }
  },
  "required": [
    "id",
    "options_schema",
    "facts_schema",
    "facts_semantics",
    "contract_sha256"
  ],
  "title": "ObserverContract",
  "type": "object"
}
```

</details>
