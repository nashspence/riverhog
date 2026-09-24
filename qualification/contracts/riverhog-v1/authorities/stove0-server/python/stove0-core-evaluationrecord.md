# stove0_core.EvaluationRecord

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationrecord:51c18da39c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-400e8f7ffa"></a>
- <a id="s-7fc4e7849b"></a>`distribution`: `stove0-server`
- <a id="s-208f491553"></a>`module`: `stove0_core`
- <a id="s-2a1986573e"></a>`name`: `EvaluationRecord`
- <a id="s-7aed46a79d"></a>`unit`: `export`

### Declared structure

- <a id="s-4d80d5535a"></a>`kind`: `"class"`
- <a id="s-7e2b9f3e1c"></a>`signature`: `"\"(*, format: Literal['stove0-evaluation-record/v1'] = 'stove0-evaluation-record/v1', definition: stove0_protocol.models.EvaluationDefinition, phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled'] = 'planning', revision: Annotated[int, Ge(ge=1)] = 1, children: tuple[stove0_core.evaluation.EvaluationChild, ...], reviews: tuple[stove0_core.evaluation.EvaluationReview, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-6a63a38759"></a>

- <a id="s-14f3254810"></a>`type`: `"object"`
- <a id="s-8e829194cb"></a>`additionalProperties`: `false`
- <a id="s-4b3bd4bd89"></a>`required`: `["definition","children"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-92e72dcadc"></a>`children` | yes | type="array"; items=([EvaluationChild](#s-d3e952e155)) |  |
| <a id="s-8a6c30e2c0"></a>`definition` | yes | [EvaluationDefinition](#s-5039df5a1d) |  |
| <a id="s-f26c2aa71f"></a>`format` | no | type="string"; const="stove0-evaluation-record/v1"; default="stove0-evaluation-record/v1" |  |
| <a id="s-6c09420113"></a>`phase` | no | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"]; default="planning" |  |
| <a id="s-8816e049b9"></a>`reviews` | no | type="array"; default=[]; items=([EvaluationReview](#s-0949b76fc9)) |  |
| <a id="s-1b28e285a9"></a>`revision` | no | type="integer"; minimum=1; default=1 |  |

##### Definitions

- [CollectionId](#s-f1a5fa9dd5)
- [CollectionRootIdentityRef](#s-9f64c45734)
- [EvaluationChild](#s-d3e952e155)
- [EvaluationDefinition](#s-5039df5a1d)
- [EvaluationMatrix](#s-f9f71920c6)
- [EvaluationReview](#s-0949b76fc9)
- [EvaluationVariant](#s-aad4dd0548)
- [JsonValue](#s-c91c28bb44)
- [OutputCollectionRef](#s-0f5082dc67)
- [RecipeIdentityRef](#s-bff1879b9c)

##### <a id="s-f1a5fa9dd5"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-dbc9d109f2"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-5867d96b71"></a>2 | not=(const="0") |

##### <a id="s-9f64c45734"></a>definition `CollectionRootIdentityRef`

- <a id="s-a0e748c6c9"></a>`type`: `"object"`
- <a id="s-a882c01719"></a>`additionalProperties`: `false`
- <a id="s-365af4acf9"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-26a8935ed9"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-94c387d81b"></a>`collection_id` | yes | [CollectionId](#s-f1a5fa9dd5) |  |
| <a id="s-e6462388ff"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d3e952e155"></a>definition `EvaluationChild`

- <a id="s-1ec88aa164"></a>`type`: `"object"`
- <a id="s-b72868ad5d"></a>`additionalProperties`: `false`
- <a id="s-2c1a9170bd"></a>`required`: `["variant_id","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6831a49f93"></a>`output` | no | anyOf=[([OutputCollectionRef](#s-0f5082dc67)); (type="null")]; default=null |  |
| <a id="s-06ca23ae2e"></a>`state` | no | type="string"; enum=["pending","active","complete","inapplicable","failed","canceled"]; default="pending" |  |
| <a id="s-97ab74eb5f"></a>`variant_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-3245286344"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5039df5a1d"></a>definition `EvaluationDefinition`

- <a id="s-119300d4fe"></a>`type`: `"object"`
- <a id="s-23189c4eb1"></a>`additionalProperties`: `false`
- <a id="s-700f773aec"></a>`required`: `["recipe","inputs","matrix","evaluation_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-be87d841a3"></a>`common_intent` | no | type="object"; additionalProperties=([JsonValue](#s-c91c28bb44)) |  |
| <a id="s-6ef0b01250"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-aa8379d898"></a>`format` | no | type="string"; const="stove0-evaluation-definition/v1"; default="stove0-evaluation-definition/v1" |  |
| <a id="s-ca75b2cd86"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-9f64c45734)); minItems=1 |  |
| <a id="s-608c54b2ce"></a>`matrix` | yes | [EvaluationMatrix](#s-f9f71920c6) |  |
| <a id="s-caa4d3d675"></a>`purpose` | no | type="string"; enum=["trial","evaluation"]; default="evaluation" |  |
| <a id="s-a339fcb0df"></a>`recipe` | yes | [RecipeIdentityRef](#s-bff1879b9c) |  |

##### <a id="s-f9f71920c6"></a>definition `EvaluationMatrix`

- <a id="s-3b73293f63"></a>`type`: `"object"`
- <a id="s-a96e1f341b"></a>`additionalProperties`: `false`
- <a id="s-521bcb9796"></a>`required`: `["variants","matrix_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d5697e6382"></a>`format` | no | type="string"; const="stove0-evaluation-matrix/v1"; default="stove0-evaluation-matrix/v1" |  |
| <a id="s-5d9c20391c"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-92d8eec76a"></a>`variants` | yes | type="array"; items=([EvaluationVariant](#s-aad4dd0548)); minItems=1 |  |

##### <a id="s-0949b76fc9"></a>definition `EvaluationReview`

- <a id="s-5a468b8dac"></a>`type`: `"object"`
- <a id="s-a65a19fcd4"></a>`additionalProperties`: `false`
- <a id="s-96b5f6e6bc"></a>`required`: `["variant_id","updated_by","updated_at"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7aa3e76d6c"></a>`note` | no | anyOf=[(type="string"; maxLength=4000); (type="null")]; default=null |  |
| <a id="s-fefe53f68f"></a>`rating` | no | anyOf=[(type="integer"; minimum=1; maximum=5); (type="null")]; default=null |  |
| <a id="s-e7588a47f4"></a>`updated_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-c97ef8426b"></a>`updated_by` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-6e0f530cc4"></a>`variant_id` | yes | type="string"; maxLength=160; minLength=1 |  |

##### <a id="s-aad4dd0548"></a>definition `EvaluationVariant`

- <a id="s-8221f3eb3e"></a>`type`: `"object"`
- <a id="s-4f46610bf8"></a>`additionalProperties`: `false`
- <a id="s-1e155b1799"></a>`required`: `["id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c94e278b1e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fa9865b3c1"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-c91c28bb44)) |  |

##### <a id="s-c91c28bb44"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-0f5082dc67"></a>definition `OutputCollectionRef`

- <a id="s-e413888804"></a>`type`: `"object"`
- <a id="s-4266eb737a"></a>`additionalProperties`: `false`
- <a id="s-71d053a35a"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-42e1cdf40a"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-feae897ebf"></a>`collection_id` | yes | [CollectionId](#s-f1a5fa9dd5) |  |
| <a id="s-0a487a9451"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-14cd80f8e0"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-bff1879b9c"></a>definition `RecipeIdentityRef`

- <a id="s-bb56b4256c"></a>`type`: `"object"`
- <a id="s-7013a515c3"></a>`additionalProperties`: `false`
- <a id="s-b92eaea144"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4a4d4481b5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1694027610"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-af1692d6b0"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [evaluation_id](stove0-core-evaluationrecord-evaluation-id.md)
- [validate_children](stove0-core-evaluationrecord-validate-children.md)

## Governing policies

- <a id="pa-c9bf4a1f4f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationRecord`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a7579e880ad6cafeaa0f1e084469fbc53330798db7c598c18119c5438d6d2be -->

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
        "EvaluationChild": {
          "additionalProperties": false,
          "properties": {
            "output": {
              "anyOf": [
                {
                  "$ref": "#/$defs/OutputCollectionRef"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "state": {
              "default": "pending",
              "enum": [
                "pending",
                "active",
                "complete",
                "inapplicable",
                "failed",
                "canceled"
              ],
              "type": "string"
            },
            "variant_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "variant_id",
            "work_id"
          ],
          "type": "object"
        },
        "EvaluationDefinition": {
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
        "EvaluationReview": {
          "additionalProperties": false,
          "properties": {
            "note": {
              "anyOf": [
                {
                  "maxLength": 4000,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "rating": {
              "anyOf": [
                {
                  "maximum": 5,
                  "minimum": 1,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "updated_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "updated_by": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "variant_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "variant_id",
            "updated_by",
            "updated_at"
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
        "OutputCollectionRef": {
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
            },
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity",
            "derivation_sha256"
          ],
          "type": "object"
        },
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
        "children": {
          "items": {
            "$ref": "#/$defs/EvaluationChild"
          },
          "type": "array"
        },
        "definition": {
          "$ref": "#/$defs/EvaluationDefinition"
        },
        "format": {
          "const": "stove0-evaluation-record/v1",
          "default": "stove0-evaluation-record/v1",
          "type": "string"
        },
        "phase": {
          "default": "planning",
          "enum": [
            "planning",
            "running",
            "partially_complete",
            "complete",
            "failed",
            "canceled"
          ],
          "type": "string"
        },
        "reviews": {
          "default": [],
          "items": {
            "$ref": "#/$defs/EvaluationReview"
          },
          "type": "array"
        },
        "revision": {
          "default": 1,
          "minimum": 1,
          "type": "integer"
        }
      },
      "required": [
        "definition",
        "children"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-evaluation-record/v1'] = 'stove0-evaluation-record/v1', definition: stove0_protocol.models.EvaluationDefinition, phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled'] = 'planning', revision: Annotated[int, Ge(ge=1)] = 1, children: tuple[stove0_core.evaluation.EvaluationChild, ...], reviews: tuple[stove0_core.evaluation.EvaluationReview, ...] = ()) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "EvaluationRecord",
  "unit": "export"
}
```

</details>
