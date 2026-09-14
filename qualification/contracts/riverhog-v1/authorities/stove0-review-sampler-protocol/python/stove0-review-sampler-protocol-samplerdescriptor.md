# stove0_review_sampler_protocol.SamplerDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerdescriptor:35fd0695c5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a5079741b4"></a>
- <a id="s-f3b8c0234f"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-d1a266cecc"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-f78ac20922"></a>`name`: `SamplerDescriptor`
- <a id="s-f6ac5b50a0"></a>`unit`: `export`

### Declared structure

- <a id="s-a111d40f30"></a>`kind`: `"class"`
- <a id="s-382d16625b"></a>`signature`: `"\"(*, protocol: Literal['stove0-review-sampler/v1'] = 'stove0-review-sampler/v1', implementation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], primary_operation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], primary_operation_contract_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], portable_intent_schema: stove0_protocol.models.JsonSchemaDocument, output_role: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-073c74adb8"></a>
- <a id="s-7890c8bd16"></a>`title`: SamplerDescriptor
- <a id="s-abfaa34213"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3e039473af"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dea685dce0"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bb1bb97ee4"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-67d7cdeb94"></a>`implementation_version` | yes | type="string"; minLength=1; maxLength=120 |  |
| <a id="s-9e5ab3e9c9"></a>`output_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-372a1f6b42"></a>`portable_intent_schema` | yes | #/$defs/JsonSchemaDocument |  |
| <a id="s-47bea75618"></a>`primary_operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-62d54a7559"></a>`primary_operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e8f6b6ba55"></a>`protocol` | no | type="string"; const="stove0-review-sampler/v1" |  |
| <a id="s-5744fc2d0d"></a>`source_revision` | yes | type="string"; minLength=1; maxLength=200 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-ae02eb99b6"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-e5a91221a3"></a>`JsonValue` | empty object |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_protocol.SamplerDescriptor.seal](stove0-review-sampler-protocol-samplerdescriptor-seal.md)
- [stove0_review_sampler_protocol.SamplerDescriptor.verify_digest](stove0-review-sampler-protocol-samplerdescriptor-verify-digest.md)

## Governing policies

- <a id="pa-3c31eecda1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerDescriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 15f16f00b19c8adf42a5a4e592dc573a124d523eddd831e5da7c03fa2cbc733d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
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
          "$ref": "#/$defs/JsonSchemaDocument"
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
    },
    "signature": "\"(*, protocol: Literal['stove0-review-sampler/v1'] = 'stove0-review-sampler/v1', implementation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], primary_operation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], primary_operation_contract_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], portable_intent_schema: stove0_protocol.models.JsonSchemaDocument, output_role: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerDescriptor",
  "unit": "export"
}
```
