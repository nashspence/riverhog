# stove0_operator_contracts.EvaluationView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationview:cccfaf14d6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0fb913ef36"></a>
- <a id="s-691e724c39"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-b51ab2d827"></a>`module`: `stove0_operator_contracts`
- <a id="s-3f1b79eda1"></a>`name`: `EvaluationView`
- <a id="s-1381c3117d"></a>`unit`: `export`

### Declared structure

- <a id="s-98d2d33f55"></a>`kind`: `"class"`
- <a id="s-f482de81ed"></a>`signature`: `"\"(*, format: Literal['stove0-evaluation-view/v1'] = 'stove0-evaluation-view/v1', evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], definition: stove0_protocol.models.EvaluationDefinition, phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=1)], children: tuple[stove0_operator_contracts.EvaluationChildView, ...], reviews: tuple[stove0_operator_contracts.EvaluationReviewView, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-b823054ed1"></a>

- <a id="s-9d43ab102f"></a>`type`: `"object"`
- <a id="s-54b062e853"></a>`additionalProperties`: `false`
- <a id="s-e2feaf0baa"></a>`required`: `["evaluation_id","definition","phase","revision","children"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-22026ef7b4"></a>`children` | yes | type="array"; items=([EvaluationChildView](#s-0d811118d7)) |  |
| <a id="s-a9d530d8ea"></a>`definition` | yes | [EvaluationDefinition](#s-e3eae98216) |  |
| <a id="s-8ff988ed5b"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2891a411d2"></a>`format` | no | type="string"; const="stove0-evaluation-view/v1"; default="stove0-evaluation-view/v1" |  |
| <a id="s-d4ccb27f02"></a>`phase` | yes | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"] |  |
| <a id="s-26307ae6fb"></a>`reviews` | no | type="array"; default=[]; items=([EvaluationReviewView](#s-90ebe1c97f)) |  |
| <a id="s-c725182efd"></a>`revision` | yes | type="integer"; minimum=1 |  |

##### Definitions

- [CollectionId](#s-a827e0799f)
- [CollectionRootRef](#s-b9ce7a1634)
- [EvaluationChildView](#s-0d811118d7)
- [EvaluationDefinition](#s-e3eae98216)
- [EvaluationMatrix](#s-feaddbff26)
- [EvaluationReviewView](#s-90ebe1c97f)
- [EvaluationVariant](#s-0055b1e45f)
- [JsonValue](#s-e8be429117)
- [OutputCollectionRef](#s-0a3cf8b181)
- [RecipeRef](#s-81eef020ae)

##### <a id="s-a827e0799f"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-889ebdcea3"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-27093eaa19"></a>2 | not=(const="0") |

##### <a id="s-b9ce7a1634"></a>definition `CollectionRootRef`

- <a id="s-fff6ab7bbc"></a>`type`: `"object"`
- <a id="s-c705dfd1eb"></a>`additionalProperties`: `false`
- <a id="s-9ee5386048"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d4d3188eeb"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7bc6c123da"></a>`collection_id` | yes | [CollectionId](#s-a827e0799f) |  |
| <a id="s-e77df73287"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0d811118d7"></a>definition `EvaluationChildView`

- <a id="s-1d004ed9eb"></a>`type`: `"object"`
- <a id="s-92cb77e130"></a>`additionalProperties`: `false`
- <a id="s-cde00cb03b"></a>`required`: `["variant_id","work_id","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6afce6a5c3"></a>`output` | no | anyOf=[([OutputCollectionRef](#s-0a3cf8b181)); (type="null")]; default=null |  |
| <a id="s-a39327fa37"></a>`state` | yes | type="string"; enum=["pending","active","complete","inapplicable","failed","canceled"] |  |
| <a id="s-dd4ac9f785"></a>`variant_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-10e9edb68c"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e3eae98216"></a>definition `EvaluationDefinition`

- <a id="s-63597020fe"></a>`type`: `"object"`
- <a id="s-bb14cd81a0"></a>`additionalProperties`: `false`
- <a id="s-37150ad625"></a>`required`: `["recipe","inputs","matrix","evaluation_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2c5547156a"></a>`common_intent` | no | type="object"; additionalProperties=([JsonValue](#s-e8be429117)) |  |
| <a id="s-d1c36dee60"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d2b0558c15"></a>`format` | no | type="string"; const="stove0-evaluation-definition/v1"; default="stove0-evaluation-definition/v1" |  |
| <a id="s-cccf8e5513"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-b9ce7a1634)); minItems=1 |  |
| <a id="s-073477436d"></a>`matrix` | yes | [EvaluationMatrix](#s-feaddbff26) |  |
| <a id="s-87b605d4f9"></a>`purpose` | no | type="string"; enum=["trial","evaluation"]; default="evaluation" |  |
| <a id="s-9fe6db4dca"></a>`recipe` | yes | [RecipeRef](#s-81eef020ae) |  |

##### <a id="s-feaddbff26"></a>definition `EvaluationMatrix`

- <a id="s-1d9760cf35"></a>`type`: `"object"`
- <a id="s-b8dd484b12"></a>`additionalProperties`: `false`
- <a id="s-9fb82df2eb"></a>`required`: `["variants","matrix_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f66cb85e78"></a>`format` | no | type="string"; const="stove0-evaluation-matrix/v1"; default="stove0-evaluation-matrix/v1" |  |
| <a id="s-7e4cb28fce"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f610440fdc"></a>`variants` | yes | type="array"; items=([EvaluationVariant](#s-0055b1e45f)); minItems=1 |  |

##### <a id="s-90ebe1c97f"></a>definition `EvaluationReviewView`

- <a id="s-fa64ce83b9"></a>`type`: `"object"`
- <a id="s-d8fa0c31b7"></a>`additionalProperties`: `false`
- <a id="s-ba07d3fa41"></a>`required`: `["variant_id","updated_by","updated_at"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c4f91233ea"></a>`note` | no | anyOf=[(type="string"; maxLength=4000; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$"); (type="null")]; default=null |  |
| <a id="s-0c831491e4"></a>`rating` | no | anyOf=[(type="integer"; minimum=1; maximum=5); (type="null")]; default=null |  |
| <a id="s-168a50aa40"></a>`updated_at` | yes | type="string"; maxLength=40; minLength=1 |  |
| <a id="s-678d2be791"></a>`updated_by` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-9510f7b1ce"></a>`variant_id` | yes | type="string"; maxLength=160; minLength=1 |  |

##### <a id="s-0055b1e45f"></a>definition `EvaluationVariant`

- <a id="s-c2e11fcdeb"></a>`type`: `"object"`
- <a id="s-0dd2de5ba0"></a>`additionalProperties`: `false`
- <a id="s-e7aa52be4f"></a>`required`: `["id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-af9a019bc4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b5a3cf90d8"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-e8be429117)) |  |

##### <a id="s-e8be429117"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-0a3cf8b181"></a>definition `OutputCollectionRef`

- <a id="s-5dda2dc624"></a>`type`: `"object"`
- <a id="s-20cdbfa4df"></a>`additionalProperties`: `false`
- <a id="s-588c67c916"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b0a238999a"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e323755668"></a>`collection_id` | yes | [CollectionId](#s-a827e0799f) |  |
| <a id="s-dbaeaefe21"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1aa13d839d"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-81eef020ae"></a>definition `RecipeRef`

- <a id="s-1d71e87a17"></a>`type`: `"object"`
- <a id="s-3d93a882d4"></a>`additionalProperties`: `false`
- <a id="s-62596882e5"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ba6e6393c0"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9d60096d6a"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-77a75395bd"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [exact_identity](stove0-operator-contracts-evaluationview-exact-identity.md)
- [from_record](stove0-operator-contracts-evaluationview-from-record.md)

## Governing policies

- <a id="pa-9f8bca2a83"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fffa3e282ed95a58a319808a14e17c25fc86376b76001a10668c40b6289c81dc -->

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
        "CollectionRootRef": {
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
        "EvaluationChildView": {
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
            "work_id",
            "state"
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
                "$ref": "#/$defs/CollectionRootRef"
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
              "$ref": "#/$defs/RecipeRef"
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
        "EvaluationReviewView": {
          "additionalProperties": false,
          "properties": {
            "note": {
              "anyOf": [
                {
                  "maxLength": 4000,
                  "minLength": 1,
                  "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
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
              "maxLength": 40,
              "minLength": 1,
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
        "RecipeRef": {
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
            "$ref": "#/$defs/EvaluationChildView"
          },
          "type": "array"
        },
        "definition": {
          "$ref": "#/$defs/EvaluationDefinition"
        },
        "evaluation_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "format": {
          "const": "stove0-evaluation-view/v1",
          "default": "stove0-evaluation-view/v1",
          "type": "string"
        },
        "phase": {
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
            "$ref": "#/$defs/EvaluationReviewView"
          },
          "type": "array"
        },
        "revision": {
          "minimum": 1,
          "type": "integer"
        }
      },
      "required": [
        "evaluation_id",
        "definition",
        "phase",
        "revision",
        "children"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-evaluation-view/v1'] = 'stove0-evaluation-view/v1', evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], definition: stove0_protocol.models.EvaluationDefinition, phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=1)], children: tuple[stove0_operator_contracts.EvaluationChildView, ...], reviews: tuple[stove0_operator_contracts.EvaluationReviewView, ...] = ()) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationView",
  "unit": "export"
}
```

</details>
