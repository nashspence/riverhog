# generated:review0-sampler: SamplerDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:review0-sampler-lib:generated-review0-sampler-samplerdescriptor:84050a419b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-c8e61d44b4"></a>

- <a id="s-8bb085c137"></a>`type`: `"object"`
- <a id="s-d0bad8c481"></a>`additionalProperties`: `false`
- <a id="s-50e32b91d5"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","primary_operation_id","primary_operation_contract_sha256","portable_intent_schema","output_role","descriptor_sha256"]`
- <a id="s-0f9c6fea94"></a>`title`: `"SamplerDescriptor"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dcde775ba7"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-204710dce9"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Image Digest" |  |
| <a id="s-5165465155"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Implementation Id" |  |
| <a id="s-5ee4a6d95e"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1; title="Implementation Version" |  |
| <a id="s-a556d83877"></a>`output_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Output Role" |  |
| <a id="s-389c475257"></a>`portable_intent_schema` | yes | [JsonSchemaValidationProfile](#s-d5cf518ea3) |  |
| <a id="s-748a6f4df7"></a>`primary_operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Primary Operation Contract Sha256" |  |
| <a id="s-39f38ceef6"></a>`primary_operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Primary Operation Id" |  |
| <a id="s-1eb6d5f0d7"></a>`protocol` | no | type="string"; const="review0-sampler/v1"; default="review0-sampler/v1"; title="Protocol" |  |
| <a id="s-e6cc5cd820"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |

### Definitions

- [JsonSchemaValidationProfile](#s-d5cf518ea3)
- [JsonValue](#s-a425e3c3a2)

### <a id="s-d5cf518ea3"></a>definition `JsonSchemaValidationProfile`

- <a id="s-ef19517667"></a>`type`: `"object"`
- <a id="s-54cc41237f"></a>`additionalProperties`: `false`
- <a id="s-7990f55715"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-71cbea4e61"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-986c771240"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-7fc1045ed0"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-3cc793a08f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-0f1d9f562e"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-a65fe11625"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-a425e3c3a2)); title="Schema" |  |

### <a id="s-a425e3c3a2"></a>definition `JsonValue`

- Accepts: any JSON value.

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field descriptor_sha256](#s-dcde775ba7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field image_digest](#s-204710dce9) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field implementation_version](#s-5ee4a6d95e) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| [field primary_operation_contract_sha256](#s-748a6f4df7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field source_revision](#s-e6cc5cd820) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:review0-sampler protocol](../process-protocol/generated-review0-sampler-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-2f907bb902"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-cada0c89ef"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:review0-sampler](../../../evidence/sources/authorities.md#src-8a54180841) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/schemas.py::sampler\_schema\_bundle](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:review0-sampler/schemas/SamplerDescriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 832b761b94ea0fac129593c143adc45a790731e631064e9216bd7e14c746e2cf -->

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
      "const": "review0-sampler/v1",
      "default": "review0-sampler/v1",
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
