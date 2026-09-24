# stove0_protocol.EvaluationDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationdefinition:89c046ee44 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1937071e44"></a>
- <a id="s-eab39ec1e6"></a>`distribution`: `stove0-protocol`
- <a id="s-1c3a591e24"></a>`module`: `stove0_protocol`
- <a id="s-d4bfa72105"></a>`name`: `EvaluationDefinition`
- <a id="s-37e10fc1cd"></a>`unit`: `export`

### Declared structure

- <a id="s-a1d27d342f"></a>`kind`: `"class"`
- <a id="s-88ddd34ca8"></a>`signature`: `"\"(*, format: Literal['stove0-evaluation-definition/v1'] = 'stove0-evaluation-definition/v1', purpose: Literal['trial', 'evaluation'] = 'evaluation', recipe: stove0_protocol.models.RecipeIdentityRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootIdentityRef, ...], MinLen(min_length=1)], common_intent: dict[str, JsonValue] = <factory>, matrix: stove0_protocol.models.EvaluationMatrix, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-a80ac8a32a"></a>

- <a id="s-9ca6faacf7"></a>`type`: `"object"`
- <a id="s-8cacdd798d"></a>`additionalProperties`: `false`
- <a id="s-27264c594e"></a>`required`: `["recipe","inputs","matrix","evaluation_id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ab8f045ab1"></a>`common_intent` | no | type="object"; additionalProperties=([JsonValue](#s-3d3dab1600)) |  |
| <a id="s-444381c456"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1329820d22"></a>`format` | no | type="string"; const="stove0-evaluation-definition/v1"; default="stove0-evaluation-definition/v1" |  |
| <a id="s-914f6dd691"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-c942642da4)); minItems=1 |  |
| <a id="s-b30fdc8921"></a>`matrix` | yes | [EvaluationMatrix](#s-b12f948340) |  |
| <a id="s-82cd0c8b05"></a>`purpose` | no | type="string"; enum=["trial","evaluation"]; default="evaluation" |  |
| <a id="s-57ae92593f"></a>`recipe` | yes | [RecipeIdentityRef](#s-6050afda11) |  |

##### Definitions

- [CollectionId](#s-b734067b50)
- [CollectionRootIdentityRef](#s-c942642da4)
- [EvaluationMatrix](#s-b12f948340)
- [EvaluationVariant](#s-19aadbf995)
- [JsonValue](#s-3d3dab1600)
- [NonnegativeDecimal](#s-1831b07c11)
- [RecipeIdentityRef](#s-6050afda11)

##### <a id="s-b734067b50"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-f1ea3c6e39"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-f479ec8a7a"></a>2 | not=(const="0") |

##### <a id="s-c942642da4"></a>definition `CollectionRootIdentityRef`

- <a id="s-845e723290"></a>`type`: `"object"`
- <a id="s-05739c4f22"></a>`additionalProperties`: `false`
- <a id="s-5316b81d46"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0ce02d60f1"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d6e283e1ae"></a>`collection_id` | yes | [CollectionId](#s-b734067b50) |  |
| <a id="s-06944c438b"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b12f948340"></a>definition `EvaluationMatrix`

- <a id="s-35711fa8e4"></a>`type`: `"object"`
- <a id="s-788257895b"></a>`additionalProperties`: `false`
- <a id="s-8e73a8548e"></a>`required`: `["variants","matrix_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-60e013fe92"></a>`format` | no | type="string"; const="stove0-evaluation-matrix/v1"; default="stove0-evaluation-matrix/v1" |  |
| <a id="s-8ff3d7b299"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7844ce7058"></a>`variants` | yes | type="array"; items=([EvaluationVariant](#s-19aadbf995)); minItems=1 |  |

##### <a id="s-19aadbf995"></a>definition `EvaluationVariant`

- <a id="s-49bad381de"></a>`type`: `"object"`
- <a id="s-702f732dc4"></a>`additionalProperties`: `false`
- <a id="s-191eea99e5"></a>`required`: `["id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-249b102d6e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-589eb91f96"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-3d3dab1600)) |  |

##### <a id="s-3d3dab1600"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-1831b07c11"></a>definition `NonnegativeDecimal`

- <a id="s-6639995b52"></a>`type`: `"string"`
- <a id="s-a5b59c850b"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-6050afda11"></a>definition `RecipeIdentityRef`

- <a id="s-e7f5a928b1"></a>`type`: `"object"`
- <a id="s-f1f1c4baf8"></a>`additionalProperties`: `false`
- <a id="s-3c8f6efb09"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-08555a0943"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5612b67e5d"></a>`revision` | yes | [NonnegativeDecimal](#s-1831b07c11); ge=1 |  |
| <a id="s-a433125f6e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_inputs](stove0-protocol-evaluationdefinition-canonical-inputs.md)
- [child_work](stove0-protocol-evaluationdefinition-child-work.md)
- [child_works](stove0-protocol-evaluationdefinition-child-works.md)
- [seal](stove0-protocol-evaluationdefinition-seal.md)
- [validate_purpose](stove0-protocol-evaluationdefinition-validate-purpose.md)
- [verify_digest](stove0-protocol-evaluationdefinition-verify-digest.md)

## Governing policies

- <a id="pa-909885833a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationDefinition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf98eb7c842f4469c7281b64af7d2d323586f88bc184da0d9fdad95d88fccec5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "allOf": [
            {
              "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
              "type": "string"
            },
            {
              "not": {
                "const": "0"
              }
            }
          ]
        },
        "CollectionRootIdentityRef": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "type": "object"
        },
        "EvaluationMatrix": {
          "additionalProperties": false,
          "properties": {
            "format": {
              "const": "stove0-evaluation-matrix/v1",
              "default": "stove0-evaluation-matrix/v1",
              "type": "string"
            },
            "matrix_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "variants": {
              "items": {
                "$ref": "#/$defs/EvaluationVariant"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "variants",
            "matrix_sha256"
          ],
          "type": "object"
        },
        "EvaluationVariant": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "parameters": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "id"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        },
        "RecipeIdentityRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "revision": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "revision",
            "sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "common_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "evaluation_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "format": {
          "const": "stove0-evaluation-definition/v1",
          "default": "stove0-evaluation-definition/v1",
          "type": "string"
        },
        "inputs": {
          "items": {
            "$ref": "#/$defs/CollectionRootIdentityRef"
          },
          "minItems": 1,
          "type": "array"
        },
        "matrix": {
          "$ref": "#/$defs/EvaluationMatrix"
        },
        "purpose": {
          "default": "evaluation",
          "enum": [
            "trial",
            "evaluation"
          ],
          "type": "string"
        },
        "recipe": {
          "$ref": "#/$defs/RecipeIdentityRef"
        }
      },
      "required": [
        "recipe",
        "inputs",
        "matrix",
        "evaluation_id"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-evaluation-definition/v1'] = 'stove0-evaluation-definition/v1', purpose: Literal['trial', 'evaluation'] = 'evaluation', recipe: stove0_protocol.models.RecipeIdentityRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootIdentityRef, ...], MinLen(min_length=1)], common_intent: dict[str, JsonValue] = <factory>, matrix: stove0_protocol.models.EvaluationMatrix, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EvaluationDefinition",
  "unit": "export"
}
```

</details>
