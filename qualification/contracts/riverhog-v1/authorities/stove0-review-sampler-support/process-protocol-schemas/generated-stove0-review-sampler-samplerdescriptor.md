# generated:stove0-review-sampler: SamplerDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-review-sampler-support:generated-stove0-review-sampler-samplerdescriptor:62b5ae9266 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-16b45491db"></a>

- <a id="s-8b47c25872"></a>`type`: `"object"`
- <a id="s-9c6dc999ae"></a>`additionalProperties`: `false`
- <a id="s-31306efb98"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","primary_operation_id","primary_operation_contract_sha256","portable_intent_schema","output_role","descriptor_sha256"]`
- <a id="s-ead5d7ac10"></a>`title`: `"SamplerDescriptor"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d853c8a810"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-aeb036e211"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Image Digest" |  |
| <a id="s-bfe0aea278"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Implementation Id" |  |
| <a id="s-cce6e0de78"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1; title="Implementation Version" |  |
| <a id="s-a94844bc44"></a>`output_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Output Role" |  |
| <a id="s-277f5d0950"></a>`portable_intent_schema` | yes | [JsonSchemaValidationProfile](#s-335353d945) |  |
| <a id="s-3657c0ee2b"></a>`primary_operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Primary Operation Contract Sha256" |  |
| <a id="s-67de29c354"></a>`primary_operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Primary Operation Id" |  |
| <a id="s-340dcf6464"></a>`protocol` | no | type="string"; const="stove0-review-sampler/v1"; default="stove0-review-sampler/v1"; title="Protocol" |  |
| <a id="s-452484d157"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |

### Definitions

- [JsonSchemaValidationProfile](#s-335353d945)
- [JsonValue](#s-30e0f038c3)

### <a id="s-335353d945"></a>definition `JsonSchemaValidationProfile`

- <a id="s-6a17ea217b"></a>`type`: `"object"`
- <a id="s-ac561dce18"></a>`additionalProperties`: `false`
- <a id="s-b4ed39c042"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-af74edf2db"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cae0e81f68"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-323e930399"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-cebfb265f4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-ef4944b21c"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-5cecf28c68"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-30e0f038c3)); title="Schema" |  |

### <a id="s-30e0f038c3"></a>definition `JsonValue`

- Accepts: any JSON value.

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field descriptor_sha256](#s-d853c8a810) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field image_digest](#s-aeb036e211) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field implementation_version](#s-cce6e0de78) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| [field primary_operation_contract_sha256](#s-3657c0ee2b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field source_revision](#s-452484d157) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:stove0-review-sampler protocol](../process-protocol/generated-stove0-review-sampler-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-1f0e7b433b"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-e8f7bb0f57"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-review-sampler](../../../evidence/sources/authorities.md#src-b47f3f4d7b) — [reference/stove0/targets/review/sampler/support/src/stove0\_review\_sampler\_support/schemas.py::sampler\_schema\_bundle](../../../../../../reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-review-sampler/schemas/SamplerDescriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91e8171126f3c367e53c23b6f492487abd19cfb530784f918527f78f85dca7ff -->

```json
{
  "$defs": {
    "JsonSchemaValidationProfile": {
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
        "profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Profile Sha256",
          "type": "string"
        },
        "schema": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Schema",
          "type": "object"
        }
      },
      "required": [
        "id",
        "profile_sha256",
        "schema"
      ],
      "title": "JsonSchemaValidationProfile",
      "type": "object"
    },
    "JsonValue": {}
  },
  "additionalProperties": false,
  "properties": {
    "descriptor_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Descriptor Sha256",
      "type": "string"
    },
    "image_digest": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Image Digest",
      "type": "string"
    },
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
    "output_role": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Output Role",
      "type": "string"
    },
    "portable_intent_schema": {
      "$ref": "#/$defs/JsonSchemaValidationProfile"
    },
    "primary_operation_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Primary Operation Contract Sha256",
      "type": "string"
    },
    "primary_operation_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Primary Operation Id",
      "type": "string"
    },
    "protocol": {
      "const": "stove0-review-sampler/v1",
      "default": "stove0-review-sampler/v1",
      "title": "Protocol",
      "type": "string"
    },
    "source_revision": {
      "maxLength": 200,
      "minLength": 1,
      "title": "Source Revision",
      "type": "string"
    }
  },
  "required": [
    "implementation_id",
    "implementation_version",
    "source_revision",
    "image_digest",
    "primary_operation_id",
    "primary_operation_contract_sha256",
    "portable_intent_schema",
    "output_role",
    "descriptor_sha256"
  ],
  "title": "SamplerDescriptor",
  "type": "object"
}
```

</details>
