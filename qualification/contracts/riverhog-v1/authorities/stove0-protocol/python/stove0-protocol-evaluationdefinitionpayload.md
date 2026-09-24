# stove0_protocol.EvaluationDefinitionPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationdefinitionpayload:7841f227f2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6616483d80"></a>
- <a id="s-70b01cddbd"></a>`distribution`: `stove0-protocol`
- <a id="s-5cb40653f2"></a>`module`: `stove0_protocol`
- <a id="s-4b9fb49410"></a>`name`: `EvaluationDefinitionPayload`
- <a id="s-11e2c6cc95"></a>`unit`: `export`

### Declared structure

- <a id="s-bd9a0ec00e"></a>`kind`: `"class"`
- <a id="s-1e450933b0"></a>`signature`: `"\"(*, format: Literal['stove0-evaluation-definition/v1'] = 'stove0-evaluation-definition/v1', purpose: Literal['trial', 'evaluation'] = 'evaluation', recipe: stove0_protocol.models.RecipeIdentityRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootIdentityRef, ...], MinLen(min_length=1)], common_intent: dict[str, JsonValue] = <factory>, matrix: stove0_protocol.models.EvaluationMatrix) -> None\""`

#### Validated model schema

<a id="s-48cb2e213f"></a>

- <a id="s-aca9784d9c"></a>`type`: `"object"`
- <a id="s-7f4f83f17e"></a>`additionalProperties`: `false`
- <a id="s-05d8c9a15f"></a>`required`: `["recipe","inputs","matrix"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-565cfb668f"></a>`common_intent` | no | type="object"; additionalProperties=([JsonValue](#s-616036bef7)) |  |
| <a id="s-9f19669d55"></a>`format` | no | type="string"; const="stove0-evaluation-definition/v1"; default="stove0-evaluation-definition/v1" |  |
| <a id="s-d7c0bf2be9"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-ecb293b790)); minItems=1 |  |
| <a id="s-826225a96a"></a>`matrix` | yes | [EvaluationMatrix](#s-582abf498d) |  |
| <a id="s-69b1cbd2d1"></a>`purpose` | no | type="string"; enum=["trial","evaluation"]; default="evaluation" |  |
| <a id="s-7e64b60961"></a>`recipe` | yes | [RecipeIdentityRef](#s-6af59312e3) |  |

##### Definitions

- [CollectionId](#s-15e342a157)
- [CollectionRootIdentityRef](#s-ecb293b790)
- [EvaluationMatrix](#s-582abf498d)
- [EvaluationVariant](#s-742251bbba)
- [JsonValue](#s-616036bef7)
- [RecipeIdentityRef](#s-6af59312e3)

##### <a id="s-15e342a157"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-b7f37af854"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-48d7ca6b71"></a>2 | not=(const="0") |

##### <a id="s-ecb293b790"></a>definition `CollectionRootIdentityRef`

- <a id="s-b4f7f6e7dd"></a>`type`: `"object"`
- <a id="s-a802fe5995"></a>`additionalProperties`: `false`
- <a id="s-a360aaad0a"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-267dbcb6e2"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a909f79626"></a>`collection_id` | yes | [CollectionId](#s-15e342a157) |  |
| <a id="s-0e5cb96965"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-582abf498d"></a>definition `EvaluationMatrix`

- <a id="s-6906a005af"></a>`type`: `"object"`
- <a id="s-5d7822398b"></a>`additionalProperties`: `false`
- <a id="s-8f859727ba"></a>`required`: `["variants","matrix_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-121c91f986"></a>`format` | no | type="string"; const="stove0-evaluation-matrix/v1"; default="stove0-evaluation-matrix/v1" |  |
| <a id="s-781d685003"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6d4fd792fe"></a>`variants` | yes | type="array"; items=([EvaluationVariant](#s-742251bbba)); minItems=1 |  |

##### <a id="s-742251bbba"></a>definition `EvaluationVariant`

- <a id="s-1807a47472"></a>`type`: `"object"`
- <a id="s-e8129017ce"></a>`additionalProperties`: `false`
- <a id="s-cd1c945ae3"></a>`required`: `["id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f1be365107"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9f1529d888"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-616036bef7)) |  |

##### <a id="s-616036bef7"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-6af59312e3"></a>definition `RecipeIdentityRef`

- <a id="s-90f52c6ce1"></a>`type`: `"object"`
- <a id="s-e66ee20949"></a>`additionalProperties`: `false`
- <a id="s-bb6c28127a"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-90ffb22711"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f90b390235"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-c3c050bd3a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_inputs](stove0-protocol-evaluationdefinitionpayload-canonical-inputs.md)
- [validate_purpose](stove0-protocol-evaluationdefinitionpayload-validate-purpose.md)

## Governing policies

- <a id="pa-b4b6a6b4e5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationDefinitionPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2026fdf52b21946b94ca83caa8b792486d16e87508524b0f1287be2ce60ba6bd -->

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
        "RecipeIdentityRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "revision": {
              "minimum": 1,
              "type": "integer"
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
        "matrix"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-evaluation-definition/v1'] = 'stove0-evaluation-definition/v1', purpose: Literal['trial', 'evaluation'] = 'evaluation', recipe: stove0_protocol.models.RecipeIdentityRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootIdentityRef, ...], MinLen(min_length=1)], common_intent: dict[str, JsonValue] = <factory>, matrix: stove0_protocol.models.EvaluationMatrix) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EvaluationDefinitionPayload",
  "unit": "export"
}
```

</details>
