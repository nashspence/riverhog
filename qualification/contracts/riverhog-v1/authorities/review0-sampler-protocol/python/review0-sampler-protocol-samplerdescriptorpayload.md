# review0_sampler_protocol.SamplerDescriptorPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerdescriptorpayload:fd4ba104cf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ece16cc64c"></a>
- <a id="s-b0b59eaf8c"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-507ba7fe2f"></a>`module`: `review0_sampler_protocol`
- <a id="s-d4f88c122e"></a>`name`: `SamplerDescriptorPayload`
- <a id="s-7b4976fe31"></a>`unit`: `export`

### Declared structure

- <a id="s-89d8514191"></a>`kind`: `"class"`
- <a id="s-9334dda13a"></a>`signature`: `"\"(*, protocol: Literal['review0-sampler/v1'] = 'review0-sampler/v1', implementation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], primary_operation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], primary_operation_contract_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], portable_intent_schema: stove0_protocol.models.JsonSchemaValidationProfile, output_role: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')]) -> None\""`

#### Validated model schema

<a id="s-f33e937267"></a>

- <a id="s-0d8c320232"></a>`type`: `"object"`
- <a id="s-eebd11fe68"></a>`additionalProperties`: `false`
- <a id="s-156fbe5039"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","primary_operation_id","primary_operation_contract_sha256","portable_intent_schema","output_role"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-83373f4cd4"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-31aab08ab8"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b900b60dcf"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-6574bbf94a"></a>`output_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b4cbd188d9"></a>`portable_intent_schema` | yes | [JsonSchemaValidationProfile](#s-6feee23be2) |  |
| <a id="s-79ede2338a"></a>`primary_operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-30e4c5a859"></a>`primary_operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-364f3fab51"></a>`protocol` | no | type="string"; const="review0-sampler/v1"; default="review0-sampler/v1" |  |
| <a id="s-383a822ec8"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |

##### Definitions

- [JsonSchemaValidationProfile](#s-6feee23be2)
- [JsonValue](#s-14b1e43917)

##### <a id="s-6feee23be2"></a>definition `JsonSchemaValidationProfile`

- <a id="s-0f8e231eca"></a>`type`: `"object"`
- <a id="s-f2ea860e4a"></a>`additionalProperties`: `false`
- <a id="s-eed8f6ecdf"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e5155df9eb"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-c383bd5824"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-d399e11965"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0498c67336"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f427e3f0ec"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-14b1e43917)) |  |

##### <a id="s-14b1e43917"></a>definition `JsonValue`

- Accepts: any JSON value.

## Governing policies

- <a id="pa-fd4879dd8e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerDescriptorPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fafa6041a8b546b78aff750563ba5c43f170a27db6262da4960ca27d76eb7cff -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonSchemaValidationProfile": {
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
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "id",
            "profile_sha256",
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
          "$ref": "#/$defs/JsonSchemaValidationProfile"
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
          "const": "review0-sampler/v1",
          "default": "review0-sampler/v1",
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
    "signature": "\"(*, protocol: Literal['review0-sampler/v1'] = 'review0-sampler/v1', implementation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], primary_operation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], primary_operation_contract_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], portable_intent_schema: stove0_protocol.models.JsonSchemaValidationProfile, output_role: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')]) -> None\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "SamplerDescriptorPayload",
  "unit": "export"
}
```

</details>
