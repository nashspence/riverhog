# stove0_operator_contracts.EvaluationPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationpage:c0d9fc40bc -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-00241bccb7"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d0005e583b"></a>`evaluations` | yes | type="array"; items=(#/$defs/EvaluationView) |  |
| <a id="s-58d16f21a2"></a>`filters` | yes | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-d6a39d0a16"></a>`next_page_token` | yes | anyOf=#/$defs/BrowsePageToken \| type="null" |  |
| <a id="s-ca42b52286"></a>`order` | yes | type="string"; enum=["asc","desc"] |  |
| <a id="s-1d79a07443"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-e74a79d0cb"></a>`sort` | yes | type="string"; enum=["updated_at","phase","evaluation_id"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-7fa86e37fe"></a>`BrowsePageToken` | type="string"; minLength=1; maxLength=8192 |
| <a id="s-1430bb8c1a"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-26f800786f"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-2d1a652e9a"></a>`EvaluationChildView` | type="object"; fields=`output`, `state`, `variant_id`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-50c7407e49"></a>`EvaluationDefinition` | type="object"; fields=`common_intent`, `evaluation_id`, `format`, `inputs`, `matrix`, `purpose`, `recipe`; additional keys=`additionalProperties`, `required` |
| <a id="s-33744e9754"></a>`EvaluationMatrix` | type="object"; fields=`format`, `matrix_sha256`, `variants`; additional keys=`additionalProperties`, `required` |
| <a id="s-374928097f"></a>`EvaluationReviewView` | type="object"; fields=`note`, `rating`, `updated_at`, `updated_by`, `variant_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-834112e22e"></a>`EvaluationVariant` | type="object"; fields=`id`, `parameters`; additional keys=`additionalProperties`, `required` |
| <a id="s-765d4e2236"></a>`EvaluationView` | type="object"; fields=`children`, `definition`, `evaluation_id`, `format`, `phase`, `reviews`, `revision`; additional keys=`additionalProperties`, `required` |
| <a id="s-1bd1daefdd"></a>`JsonValue` | empty object |
| <a id="s-2262efd035"></a>`OutputCollectionRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `derivation_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-30bd6e21a2"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [from_page](stove0-operator-contracts-evaluationpage-from-page.md)

## Governing policies

- <a id="pa-488caef597"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationPage`

### Exact owned JSON

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
