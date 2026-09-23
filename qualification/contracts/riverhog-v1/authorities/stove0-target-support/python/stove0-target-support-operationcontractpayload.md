# stove0_target_support.OperationContractPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-operationcontractpayload:89e8287214 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3ba167db03"></a>
- <a id="s-8f54560d67"></a>`distribution`: `stove0-target-support`
- <a id="s-f507db26e5"></a>`module`: `stove0_target_support`
- <a id="s-380f17074d"></a>`name`: `OperationContractPayload`
- <a id="s-c0c49bf4d5"></a>`unit`: `export`

### Declared structure

- <a id="s-26a3eaaf41"></a>`kind`: `"class"`
- <a id="s-09968d0407"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', intent_schema: stove0_protocol.models.JsonSchemaValidationProfile, intent_semantics: stove0_protocol.models.SemanticValidationProfile, inputs: Annotated[tuple[stove0_target_protocol.protocol.InputArtifactContract, ...], MinLen(min_length=1)], outputs: tuple[stove0_target_protocol.protocol.OutputArtifactContract, ...] = (), effect_receipt_schema: stove0_protocol.models.JsonSchemaValidationProfile \| None = None, source_retirement_permitted: bool = False) -> None\""`

#### Validated model schema

<a id="s-56e80101a3"></a>

- <a id="s-fdf737d145"></a>`type`: `"object"`
- <a id="s-d07b29459c"></a>`additionalProperties`: `false`
- <a id="s-6ffef082c2"></a>`required`: `["id","intent_schema","intent_semantics","inputs"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-89641b93bb"></a>`effect_receipt_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-182cb6aff7)); (type="null")]; default=null |  |
| <a id="s-ee9709cd7d"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ee121774eb"></a>`inputs` | yes | type="array"; items=([InputArtifactContract](#s-67a59ddc66)); minItems=1 |  |
| <a id="s-056f284e77"></a>`intent_schema` | yes | [JsonSchemaValidationProfile](#s-182cb6aff7) |  |
| <a id="s-411f86662c"></a>`intent_semantics` | yes | [SemanticValidationProfile](#s-572cc061ca) |  |
| <a id="s-075a8005e6"></a>`outputs` | no | type="array"; default=[]; items=([OutputArtifactContract](#s-07454fbe02)) |  |
| <a id="s-2da8f87ec1"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-894ebe49e9"></a>`source_retirement_permitted` | no | type="boolean"; default=false |  |

##### Definitions

- [InputArtifactContract](#s-67a59ddc66)
- [JsonSchemaValidationProfile](#s-182cb6aff7)
- [JsonValue](#s-e6b231b9a1)
- [OutputArtifactContract](#s-07454fbe02)
- [SemanticValidationProfile](#s-572cc061ca)

##### <a id="s-67a59ddc66"></a>definition `InputArtifactContract`

- <a id="s-afb8e415c8"></a>`type`: `"object"`
- <a id="s-3aea711c39"></a>`additionalProperties`: `false`
- <a id="s-0d210dccc9"></a>`required`: `["role"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7598cfb825"></a>`allowed_dispositions` | no | anyOf=[(type="array"; items=(type="string"; enum=["transformed","preserved","omitted","rejected"])); (type="null")]; default=null |  |
| <a id="s-3970369ae1"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |
| <a id="s-43c9d2e6b9"></a>`minimum` | no | type="integer"; minimum=0; default=1 |  |
| <a id="s-41c442ba7f"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-182cb6aff7"></a>definition `JsonSchemaValidationProfile`

- <a id="s-ac30eabb3c"></a>`type`: `"object"`
- <a id="s-90f65900d5"></a>`additionalProperties`: `false`
- <a id="s-86a4684ca2"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-35696f4529"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-dd77f90895"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-337bd61c79"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b9b12ec738"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1124253153"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-e6b231b9a1)) |  |

##### <a id="s-e6b231b9a1"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-07454fbe02"></a>definition `OutputArtifactContract`

- <a id="s-671c871ea1"></a>`type`: `"object"`
- <a id="s-4c74a8a580"></a>`additionalProperties`: `false`
- <a id="s-2f23cf3791"></a>`required`: `["role","derived_from_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fb8b771ead"></a>`derived_from_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |
| <a id="s-b50e8b3fd8"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |
| <a id="s-c859ccf772"></a>`minimum` | no | type="integer"; minimum=0; default=1 |  |
| <a id="s-8b8fbe588c"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-572cc061ca"></a>definition `SemanticValidationProfile`

- <a id="s-e4e70cb6ce"></a>`type`: `"object"`
- <a id="s-ff078675fb"></a>`additionalProperties`: `false`
- <a id="s-53007d8294"></a>`required`: `["id","rules","profile_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7f608a1594"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-06ec74afaa"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7c7e4fc40b"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-74a8818843"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

## Maintained corroboration

### Related interface records

- [validate_roles](stove0-target-support-operationcontractpayload-validate-roles.md)
- [bind_semantic_conformance_vectors](stove0-target-support-operationcontractpayload-bind-semantic-conformance-vectors.md)

## Governing policies

- <a id="pa-4c129baa8a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.OperationContractPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c66782e97d97bac7aa328a1c8e980ebee5aabd9ec95f88f53c9b04b2eb5fa49 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "InputArtifactContract": {
          "additionalProperties": false,
          "properties": {
            "allowed_dispositions": {
              "anyOf": [
                {
                  "items": {
                    "enum": [
                      "transformed",
                      "preserved",
                      "omitted",
                      "rejected"
                    ],
                    "type": "string"
                  },
                  "type": "array"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "maximum": {
              "anyOf": [
                {
                  "minimum": 1,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "minimum": {
              "default": 1,
              "minimum": 0,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role"
          ],
          "type": "object"
        },
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
        "JsonValue": {},
        "OutputArtifactContract": {
          "additionalProperties": false,
          "properties": {
            "derived_from_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            },
            "maximum": {
              "anyOf": [
                {
                  "minimum": 1,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "minimum": {
              "default": 1,
              "minimum": 0,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role",
            "derived_from_roles"
          ],
          "type": "object"
        },
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
              "default": null
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "rules": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "id",
            "rules",
            "profile_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "effect_receipt_schema": {
          "anyOf": [
            {
              "$ref": "#/$defs/JsonSchemaValidationProfile"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "inputs": {
          "items": {
            "$ref": "#/$defs/InputArtifactContract"
          },
          "minItems": 1,
          "type": "array"
        },
        "intent_schema": {
          "$ref": "#/$defs/JsonSchemaValidationProfile"
        },
        "intent_semantics": {
          "$ref": "#/$defs/SemanticValidationProfile"
        },
        "outputs": {
          "default": [],
          "items": {
            "$ref": "#/$defs/OutputArtifactContract"
          },
          "type": "array"
        },
        "result_kind": {
          "default": "collection",
          "enum": [
            "collection",
            "external-effect"
          ],
          "type": "string"
        },
        "source_retirement_permitted": {
          "default": false,
          "type": "boolean"
        }
      },
      "required": [
        "id",
        "intent_schema",
        "intent_semantics",
        "inputs"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', intent_schema: stove0_protocol.models.JsonSchemaValidationProfile, intent_semantics: stove0_protocol.models.SemanticValidationProfile, inputs: Annotated[tuple[stove0_target_protocol.protocol.InputArtifactContract, ...], MinLen(min_length=1)], outputs: tuple[stove0_target_protocol.protocol.OutputArtifactContract, ...] = (), effect_receipt_schema: stove0_protocol.models.JsonSchemaValidationProfile | None = None, source_retirement_permitted: bool = False) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "OperationContractPayload",
  "unit": "export"
}
```

</details>
