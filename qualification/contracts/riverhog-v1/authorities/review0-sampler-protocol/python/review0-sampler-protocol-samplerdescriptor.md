# review0_sampler_protocol.SamplerDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerdescriptor:3fe96453d5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c5ec24b325"></a>
- <a id="s-7fd45a67e0"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-d34f7fb116"></a>`module`: `review0_sampler_protocol`
- <a id="s-ea08dfd2e7"></a>`name`: `SamplerDescriptor`
- <a id="s-589f6c141f"></a>`unit`: `export`

### Declared structure

- <a id="s-633b00a0f6"></a>`kind`: `"class"`
- <a id="s-548c879712"></a>`signature`: `"\"(*, protocol: Literal['review0-sampler/v1'] = 'review0-sampler/v1', implementation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^sha256:[0-9a-f]{64}$', ascii_only=None)], primary_operation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], primary_operation_contract_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], portable_intent_schema: stove0_protocol.models.JsonSchemaValidationProfile, output_role: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-563ccb8b0d"></a>

- <a id="s-1f9794176f"></a>`type`: `"object"`
- <a id="s-9afd856775"></a>`additionalProperties`: `false`
- <a id="s-21a5303a96"></a>`required`: `["implementation_id","implementation_version","source_revision","image_id","primary_operation_id","primary_operation_contract_sha256","portable_intent_schema","output_role","descriptor_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f43477a970"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-51faa2f42f"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$" |  |
| <a id="s-a9c014e347"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-500a984899"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-3eb20ae8f1"></a>`output_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-73b3ce3714"></a>`portable_intent_schema` | yes | [JsonSchemaValidationProfile](#s-f39490f927) |  |
| <a id="s-b20cc1341d"></a>`primary_operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c3ec0a9f8b"></a>`primary_operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-188e8474a4"></a>`protocol` | no | type="string"; const="review0-sampler/v1"; default="review0-sampler/v1" |  |
| <a id="s-477f1cd513"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |

##### Definitions

- [JsonSchemaValidationProfile](#s-f39490f927)
- [JsonValue](#s-b9359e3a27)

##### <a id="s-f39490f927"></a>definition `JsonSchemaValidationProfile`

- <a id="s-4a21eb03cb"></a>`type`: `"object"`
- <a id="s-585ecb8fd3"></a>`additionalProperties`: `false`
- <a id="s-f49d79ee19"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-329967965c"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-6d137e7723"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-c2550b388f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-116175246c"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3743af622f"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-b9359e3a27)) |  |

##### <a id="s-b9359e3a27"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [verify_digest](review0-sampler-protocol-samplerdescriptor-verify-digest.md)
- [seal](review0-sampler-protocol-samplerdescriptor-seal.md)

## Governing policies

- <a id="pa-cb34135814"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerDescriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eba043b0b0caaa1be656096fff83e3e7ad522b137ca73322f62ff11880dee43b -->

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
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "image_id": {
          "pattern": "^sha256:[0-9a-f]{64}$",
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
        "image_id",
        "primary_operation_id",
        "primary_operation_contract_sha256",
        "portable_intent_schema",
        "output_role",
        "descriptor_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, protocol: Literal['review0-sampler/v1'] = 'review0-sampler/v1', implementation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^sha256:[0-9a-f]{64}$', ascii_only=None)], primary_operation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], primary_operation_contract_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], portable_intent_schema: stove0_protocol.models.JsonSchemaValidationProfile, output_role: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "SamplerDescriptor",
  "unit": "export"
}
```

</details>
