# stove0_operator_contracts.EvaluationPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationpage:c0d9fc40bc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0d6ed7b84c"></a>
- <a id="s-4ba1e63d2d"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-a22fde2f64"></a>`module`: `stove0_operator_contracts`
- <a id="s-03ded66168"></a>`name`: `EvaluationPage`
- <a id="s-84c3be3ae2"></a>`unit`: `export`

### Declared structure

- <a id="s-b2a533e40f"></a>`kind`: `"class"`
- <a id="s-5b0ab4bc84"></a>`signature`: `"\"(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken \| None, sort: Literal['updated_at', 'phase', 'evaluation_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], evaluations: tuple[stove0_operator_contracts.EvaluationView, ...]) -> None\""`

#### Validated model schema

<a id="s-8e30b146bb"></a>

- <a id="s-00241bccb7"></a>`type`: `"object"`
- <a id="s-66b300f9bb"></a>`additionalProperties`: `false`
- <a id="s-6cdfc12e54"></a>`required`: `["page_size","next_page_token","sort","order","filters","evaluations"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d0005e583b"></a>`evaluations` | yes | type="array"; items=([EvaluationView](#s-765d4e2236)) |  |
| <a id="s-58d16f21a2"></a>`filters` | yes | type="object"; additionalProperties=([JsonValue](#s-1bd1daefdd)) |  |
| <a id="s-d6a39d0a16"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](#s-7fa86e37fe)); (type="null")] |  |
| <a id="s-ca42b52286"></a>`order` | yes | type="string"; enum=["asc","desc"] |  |
| <a id="s-1d79a07443"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-e74a79d0cb"></a>`sort` | yes | type="string"; enum=["updated_at","phase","evaluation_id"] |  |

##### Definitions

- [BrowsePageToken](#s-7fa86e37fe)
- [CollectionId](#s-1430bb8c1a)
- [CollectionRootRef](#s-26f800786f)
- [EvaluationChildView](#s-2d1a652e9a)
- [EvaluationDefinition](#s-50c7407e49)
- [EvaluationMatrix](#s-33744e9754)
- [EvaluationReviewView](#s-374928097f)
- [EvaluationVariant](#s-834112e22e)
- [EvaluationView](#s-765d4e2236)
- [JsonValue](#s-1bd1daefdd)
- [OutputCollectionRef](#s-2262efd035)
- [RecipeRef](#s-30bd6e21a2)

##### <a id="s-7fa86e37fe"></a>definition `BrowsePageToken`

- <a id="s-22e21a07c7"></a>`type`: `"string"`
- <a id="s-8e3c5b25c5"></a>`maxLength`: `8192`
- <a id="s-bcf5781059"></a>`minLength`: `1`

##### <a id="s-1430bb8c1a"></a>definition `CollectionId`

- <a id="s-f0acd99e45"></a>`type`: `"integer"`
- <a id="s-3bb2012104"></a>`minimum`: `1`

##### <a id="s-26f800786f"></a>definition `CollectionRootRef`

- <a id="s-d26df9a818"></a>`type`: `"object"`
- <a id="s-50fe30b8c2"></a>`additionalProperties`: `false`
- <a id="s-9c88e7a24e"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2e64957464"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-31ec573ffb"></a>`collection_id` | yes | [CollectionId](#s-1430bb8c1a) |  |
| <a id="s-b1f87b28cb"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-2d1a652e9a"></a>definition `EvaluationChildView`

- <a id="s-86c23f94c1"></a>`type`: `"object"`
- <a id="s-357764ccbf"></a>`additionalProperties`: `false`
- <a id="s-c1d8a09141"></a>`required`: `["variant_id","work_id","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e7831c309d"></a>`output` | no | anyOf=[([OutputCollectionRef](#s-2262efd035)); (type="null")]; default=null |  |
| <a id="s-65f0830ec5"></a>`state` | yes | type="string"; enum=["pending","active","complete","inapplicable","failed","canceled"] |  |
| <a id="s-48b2edf10f"></a>`variant_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-ebfda9d490"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-50c7407e49"></a>definition `EvaluationDefinition`

- <a id="s-9e5631729c"></a>`type`: `"object"`
- <a id="s-0065b88ed6"></a>`additionalProperties`: `false`
- <a id="s-13aaf85427"></a>`required`: `["recipe","inputs","matrix","evaluation_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d09ffb6f62"></a>`common_intent` | no | type="object"; additionalProperties=([JsonValue](#s-1bd1daefdd)) |  |
| <a id="s-44b5852aa5"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0c970d5062"></a>`format` | no | type="string"; const="stove0-evaluation-definition/v1"; default="stove0-evaluation-definition/v1" |  |
| <a id="s-abd6dcd3b5"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-26f800786f)); minItems=1 |  |
| <a id="s-2f7215d494"></a>`matrix` | yes | [EvaluationMatrix](#s-33744e9754) |  |
| <a id="s-c1c94d3773"></a>`purpose` | no | type="string"; enum=["trial","evaluation"]; default="evaluation" |  |
| <a id="s-81a086e7bd"></a>`recipe` | yes | [RecipeRef](#s-30bd6e21a2) |  |

##### <a id="s-33744e9754"></a>definition `EvaluationMatrix`

- <a id="s-b9a187aaf7"></a>`type`: `"object"`
- <a id="s-15e7958f36"></a>`additionalProperties`: `false`
- <a id="s-5e03ee5706"></a>`required`: `["variants","matrix_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0c8665c284"></a>`format` | no | type="string"; const="stove0-evaluation-matrix/v1"; default="stove0-evaluation-matrix/v1" |  |
| <a id="s-0ee3e7c328"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-435863025f"></a>`variants` | yes | type="array"; items=([EvaluationVariant](#s-834112e22e)); minItems=1 |  |

##### <a id="s-374928097f"></a>definition `EvaluationReviewView`

- <a id="s-585e103ffb"></a>`type`: `"object"`
- <a id="s-02731bef40"></a>`additionalProperties`: `false`
- <a id="s-2b6aa3785f"></a>`required`: `["variant_id","updated_by","updated_at"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-46d76fda46"></a>`note` | no | anyOf=[(type="string"; maxLength=4000; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$"); (type="null")]; default=null |  |
| <a id="s-084eb566ef"></a>`rating` | no | anyOf=[(type="integer"; minimum=1; maximum=5); (type="null")]; default=null |  |
| <a id="s-2f51e798d4"></a>`updated_at` | yes | type="string"; maxLength=40; minLength=1 |  |
| <a id="s-e8ce269d74"></a>`updated_by` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-b933fef00a"></a>`variant_id` | yes | type="string"; maxLength=160; minLength=1 |  |

##### <a id="s-834112e22e"></a>definition `EvaluationVariant`

- <a id="s-55b26230a0"></a>`type`: `"object"`
- <a id="s-fb03d80df9"></a>`additionalProperties`: `false`
- <a id="s-b2a3486532"></a>`required`: `["id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9c7c18ebae"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ea11891669"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-1bd1daefdd)) |  |

##### <a id="s-765d4e2236"></a>definition `EvaluationView`

- <a id="s-3bdd575dad"></a>`type`: `"object"`
- <a id="s-37145d45c4"></a>`additionalProperties`: `false`
- <a id="s-a028ae2156"></a>`required`: `["evaluation_id","definition","phase","revision","children"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2e74cac3de"></a>`children` | yes | type="array"; items=([EvaluationChildView](#s-2d1a652e9a)) |  |
| <a id="s-8665581dde"></a>`definition` | yes | [EvaluationDefinition](#s-50c7407e49) |  |
| <a id="s-ba9b980fd1"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d4867dc15a"></a>`format` | no | type="string"; const="stove0-evaluation-view/v1"; default="stove0-evaluation-view/v1" |  |
| <a id="s-19d2be06a2"></a>`phase` | yes | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"] |  |
| <a id="s-8f55ec244e"></a>`reviews` | no | type="array"; default=[]; items=([EvaluationReviewView](#s-374928097f)) |  |
| <a id="s-cfe0ee5af5"></a>`revision` | yes | type="integer"; minimum=1 |  |

##### <a id="s-1bd1daefdd"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-2262efd035"></a>definition `OutputCollectionRef`

- <a id="s-8b556ee810"></a>`type`: `"object"`
- <a id="s-abcc87a2e7"></a>`additionalProperties`: `false`
- <a id="s-0df1c8cdcb"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dde2e8d438"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7f10d5b2e9"></a>`collection_id` | yes | [CollectionId](#s-1430bb8c1a) |  |
| <a id="s-18017404fa"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-81b7a22443"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-30bd6e21a2"></a>definition `RecipeRef`

- <a id="s-74ccad4019"></a>`type`: `"object"`
- <a id="s-2d3617a41b"></a>`additionalProperties`: `false`
- <a id="s-f5a980a27f"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5295aeaec3"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-830a177d76"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-766378c604"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [from_page](stove0-operator-contracts-evaluationpage-from-page.md)

## Governing policies

- <a id="pa-488caef597"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9e0e1a8a32f87092720808ce78a53a32d3c97c5d65542679d5c70350460a7498 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "BrowsePageToken": {
          "maxLength": 8192,
          "minLength": 1,
          "type": "string"
        },
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
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
        "EvaluationView": {
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
        "evaluations": {
          "items": {
            "$ref": "#/$defs/EvaluationView"
          },
          "type": "array"
        },
        "filters": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "next_page_token": {
          "anyOf": [
            {
              "$ref": "#/$defs/BrowsePageToken"
            },
            {
              "type": "null"
            }
          ]
        },
        "order": {
          "enum": [
            "asc",
            "desc"
          ],
          "type": "string"
        },
        "page_size": {
          "maximum": 100,
          "minimum": 1,
          "type": "integer"
        },
        "sort": {
          "enum": [
            "updated_at",
            "phase",
            "evaluation_id"
          ],
          "type": "string"
        }
      },
      "required": [
        "page_size",
        "next_page_token",
        "sort",
        "order",
        "filters",
        "evaluations"
      ],
      "type": "object"
    },
    "signature": "\"(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, sort: Literal['updated_at', 'phase', 'evaluation_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], evaluations: tuple[stove0_operator_contracts.EvaluationView, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationPage",
  "unit": "export"
}
```

</details>
