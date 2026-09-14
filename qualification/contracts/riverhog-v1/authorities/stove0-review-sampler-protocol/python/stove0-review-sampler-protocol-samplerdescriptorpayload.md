# stove0_review_sampler_protocol.SamplerDescriptorPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerdes-9ac4beeaeb:573700b60e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-be5050227d"></a>
- <a id="s-1ef122b66c"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-b388fb21d8"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-0973993f47"></a>`name`: `SamplerDescriptorPayload`
- <a id="s-d73df67689"></a>`unit`: `export`

### Declared structure

- <a id="s-6b4cb3b4cd"></a>`kind`: `"class"`
- <a id="s-fcbab98094"></a>`signature`: `"\"(*, protocol: Literal['stove0-review-sampler/v1'] = 'stove0-review-sampler/v1', implementation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], primary_operation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], primary_operation_contract_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], portable_intent_schema: stove0_protocol.models.JsonSchemaDocument, output_role: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')]) -> None\""`

#### Validated model schema

<a id="s-c0efceaa62"></a>
- <a id="s-3a57a2f7fc"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-152ea9bf08"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8eee09da6b"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5e835e1a2a"></a>`implementation_version` | yes | type="string"; minLength=1; maxLength=120 |  |
| <a id="s-15d8b40cd2"></a>`output_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d6ef8863f9"></a>`portable_intent_schema` | yes | #/$defs/JsonSchemaDocument |  |
| <a id="s-4358dcee2a"></a>`primary_operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7a44c4f6ae"></a>`primary_operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e440ee6d41"></a>`protocol` | no | type="string"; const="stove0-review-sampler/v1" |  |
| <a id="s-bb34d4bb28"></a>`source_revision` | yes | type="string"; minLength=1; maxLength=200 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-431124c079"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-5f90097102"></a>`JsonValue` | empty object |

## Governing policies

- <a id="pa-a568837c13"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerDescriptorPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ed68fc39401cb5c72faa8eef884c2c1bfd36d4593c5912c3bcacb548636cbe72 -->

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
              "type": "string"
            },
            "format_policy": {
              "const": "annotation-only",
              "default": "annotation-only",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256",
            "schema"
          ],
          "type": "object"
        },
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "image_digest": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "implementation_version": {
          "maxLength": 120,
          "minLength": 1,
          "type": "string"
        },
        "output_role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "portable_intent_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
        },
        "primary_operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "primary_operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "protocol": {
          "const": "stove0-review-sampler/v1",
          "default": "stove0-review-sampler/v1",
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
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
        "output_role"
      ],
      "type": "object"
    },
    "signature": "\"(*, protocol: Literal['stove0-review-sampler/v1'] = 'stove0-review-sampler/v1', implementation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], primary_operation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], primary_operation_contract_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], portable_intent_schema: stove0_protocol.models.JsonSchemaDocument, output_role: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')]) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerDescriptorPayload",
  "unit": "export"
}
```
