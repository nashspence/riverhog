# stove0_target_support.OperationContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-operationcontract:9b335ba1b0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f90eb7812"></a>
- <a id="s-934ffb10b9"></a>`distribution`: `stove0-target-support`
- <a id="s-96fae777c7"></a>`module`: `stove0_target_support`
- <a id="s-32fd9a4101"></a>`name`: `OperationContract`
- <a id="s-cc1165a6cd"></a>`unit`: `export`

### Declared structure

- <a id="s-b42b9994c5"></a>`kind`: `"class"`
- <a id="s-12aac20b1f"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', intent_schema: stove0_protocol.models.JsonSchemaValidationProfile, intent_semantics: stove0_protocol.models.SemanticValidationProfile, inputs: Annotated[tuple[stove0_target_protocol.protocol.InputArtifactContract, ...], MinLen(min_length=1)], outputs: tuple[stove0_target_protocol.protocol.OutputArtifactContract, ...] = (), effect_receipt_schema: stove0_protocol.models.JsonSchemaValidationProfile \| None = None, source_retirement_permitted: bool = False, contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-d204d1671d"></a>

- <a id="s-b614f3dca6"></a>`type`: `"object"`
- <a id="s-bf192fa7d4"></a>`additionalProperties`: `false`
- <a id="s-386e75ddf8"></a>`required`: `["id","intent_schema","intent_semantics","inputs","contract_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d4204881ca"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-79663b1644"></a>`effect_receipt_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-92a2e801a4)); (type="null")]; default=null |  |
| <a id="s-9f814a200c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8f2b6e835a"></a>`inputs` | yes | type="array"; items=([InputArtifactContract](#s-b21b5e179c)); minItems=1 |  |
| <a id="s-cbe38370b1"></a>`intent_schema` | yes | [JsonSchemaValidationProfile](#s-92a2e801a4) |  |
| <a id="s-fa92b9b1e7"></a>`intent_semantics` | yes | [SemanticValidationProfile](#s-504fefc469) |  |
| <a id="s-c1a676a0b9"></a>`outputs` | no | type="array"; default=[]; items=([OutputArtifactContract](#s-748c022692)) |  |
| <a id="s-30f320c740"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-410910b42d"></a>`source_retirement_permitted` | no | type="boolean"; default=false |  |

##### Definitions

- [InputArtifactContract](#s-b21b5e179c)
- [JsonSchemaValidationProfile](#s-92a2e801a4)
- [JsonValue](#s-571e9176e1)
- [OutputArtifactContract](#s-748c022692)
- [SemanticValidationProfile](#s-504fefc469)

##### <a id="s-b21b5e179c"></a>definition `InputArtifactContract`

- <a id="s-ff65092c6b"></a>`type`: `"object"`
- <a id="s-f92a6ed843"></a>`additionalProperties`: `false`
- <a id="s-ea587ed6fa"></a>`required`: `["role"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-82cdd663c9"></a>`allowed_dispositions` | no | anyOf=[(type="array"; items=(type="string"; enum=["transformed","preserved","omitted","rejected"])); (type="null")]; default=null |  |
| <a id="s-d9437e84e6"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |
| <a id="s-37d3bebbe7"></a>`minimum` | no | type="integer"; minimum=0; default=1 |  |
| <a id="s-843ceff0b6"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-92a2e801a4"></a>definition `JsonSchemaValidationProfile`

- <a id="s-e1bf6e2149"></a>`type`: `"object"`
- <a id="s-70ce8a297f"></a>`additionalProperties`: `false`
- <a id="s-aacf2603d4"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dd0878254f"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-c68338371e"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-309e03132e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-477ce0e377"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9a6374c9ed"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-571e9176e1)) |  |

##### <a id="s-571e9176e1"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-748c022692"></a>definition `OutputArtifactContract`

- <a id="s-e3f5e1a66a"></a>`type`: `"object"`
- <a id="s-74026a2a93"></a>`additionalProperties`: `false`
- <a id="s-d15704b048"></a>`required`: `["role","derived_from_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8aedf65829"></a>`derived_from_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |
| <a id="s-1f5c4e363b"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |
| <a id="s-ee9c66e92d"></a>`minimum` | no | type="integer"; minimum=0; default=1 |  |
| <a id="s-5648043d48"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-504fefc469"></a>definition `SemanticValidationProfile`

- <a id="s-f84ab55b6c"></a>`type`: `"object"`
- <a id="s-3d18694df8"></a>`additionalProperties`: `false`
- <a id="s-6fa7ddd84c"></a>`required`: `["id","rules","profile_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fb31e3da1e"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-1f5fd9a3cd"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5c677589c1"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7b40b01b3a"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

## Maintained corroboration

### Related interface records

- [bind_semantic_conformance_vectors](stove0-target-support-operationcontract-bind-semantic-conformance-vectors.md)
- [seal](stove0-target-support-operationcontract-seal.md)
- [validate_roles](stove0-target-support-operationcontract-validate-roles.md)
- [verify_digest](stove0-target-support-operationcontract-verify-digest.md)

## Governing policies

- <a id="pa-5b5e99398e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.OperationContract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9895d94ffeb5bf9564881e39188f12c83af94e68c978b568b8caa311824cd993 -->

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
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
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
        "inputs",
        "contract_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', intent_schema: stove0_protocol.models.JsonSchemaValidationProfile, intent_semantics: stove0_protocol.models.SemanticValidationProfile, inputs: Annotated[tuple[stove0_target_protocol.protocol.InputArtifactContract, ...], MinLen(min_length=1)], outputs: tuple[stove0_target_protocol.protocol.OutputArtifactContract, ...] = (), effect_receipt_schema: stove0_protocol.models.JsonSchemaValidationProfile | None = None, source_retirement_permitted: bool = False, contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "OperationContract",
  "unit": "export"
}
```

</details>
