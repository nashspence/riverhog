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
- <a id="s-3aeb78cbc0"></a>`title`: EvaluationPage
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

- [stove0_operator_contracts.EvaluationPage.from_page](stove0-operator-contracts-evaluationpage-from-page.md)

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

<!-- exact-contract-value: 18be8ed033017be3b6dae4cadc2e30dd49b3ee9afb8e32335b6fa6d193ca66a5 -->

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
              "title": "Archive Root Sha256",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Content Identity",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "title": "CollectionRootRef",
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
              "title": "State",
              "type": "string"
            },
            "variant_id": {
              "maxLength": 160,
              "minLength": 1,
              "title": "Variant Id",
              "type": "string"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Work Id",
              "type": "string"
            }
          },
          "required": [
            "variant_id",
            "work_id",
            "state"
          ],
          "title": "EvaluationChildView",
          "type": "object"
        },
        "EvaluationDefinition": {
          "additionalProperties": false,
          "properties": {
            "common_intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "title": "Common Intent",
              "type": "object"
            },
            "evaluation_id": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Evaluation Id",
              "type": "string"
            },
            "format": {
              "const": "stove0-evaluation-definition/v1",
              "default": "stove0-evaluation-definition/v1",
              "title": "Format",
              "type": "string"
            },
            "inputs": {
              "items": {
                "$ref": "#/$defs/CollectionRootRef"
              },
              "minItems": 1,
              "title": "Inputs",
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
              "title": "Purpose",
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
          "title": "EvaluationDefinition",
          "type": "object"
        },
        "EvaluationMatrix": {
          "additionalProperties": false,
          "properties": {
            "format": {
              "const": "stove0-evaluation-matrix/v1",
              "default": "stove0-evaluation-matrix/v1",
              "title": "Format",
              "type": "string"
            },
            "matrix_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Matrix Sha256",
              "type": "string"
            },
            "variants": {
              "items": {
                "$ref": "#/$defs/EvaluationVariant"
              },
              "minItems": 1,
              "title": "Variants",
              "type": "array"
            }
          },
          "required": [
            "variants",
            "matrix_sha256"
          ],
          "title": "EvaluationMatrix",
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
              "default": null,
              "title": "Note"
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
              "default": null,
              "title": "Rating"
            },
            "updated_at": {
              "maxLength": 40,
              "minLength": 1,
              "title": "Updated At",
              "type": "string"
            },
            "updated_by": {
              "maxLength": 160,
              "minLength": 1,
              "title": "Updated By",
              "type": "string"
            },
            "variant_id": {
              "maxLength": 160,
              "minLength": 1,
              "title": "Variant Id",
              "type": "string"
            }
          },
          "required": [
            "variant_id",
            "updated_by",
            "updated_at"
          ],
          "title": "EvaluationReviewView",
          "type": "object"
        },
        "EvaluationVariant": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Id",
              "type": "string"
            },
            "parameters": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "title": "Parameters",
              "type": "object"
            }
          },
          "required": [
            "id"
          ],
          "title": "EvaluationVariant",
          "type": "object"
        },
        "EvaluationView": {
          "additionalProperties": false,
          "description": "Operator projection of a materialized evaluation, not its identity.",
          "properties": {
            "children": {
              "items": {
                "$ref": "#/$defs/EvaluationChildView"
              },
              "title": "Children",
              "type": "array"
            },
            "definition": {
              "$ref": "#/$defs/EvaluationDefinition"
            },
            "evaluation_id": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Evaluation Id",
              "type": "string"
            },
            "format": {
              "const": "stove0-evaluation-view/v1",
              "default": "stove0-evaluation-view/v1",
              "title": "Format",
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
              "title": "Phase",
              "type": "string"
            },
            "reviews": {
              "default": [],
              "items": {
                "$ref": "#/$defs/EvaluationReviewView"
              },
              "title": "Reviews",
              "type": "array"
            },
            "revision": {
              "minimum": 1,
              "title": "Revision",
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
          "title": "EvaluationView",
          "type": "object"
        },
        "JsonValue": {},
        "OutputCollectionRef": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Archive Root Sha256",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Content Identity",
              "type": "string"
            },
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Derivation Sha256",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity",
            "derivation_sha256"
          ],
          "title": "OutputCollectionRef",
          "type": "object"
        },
        "RecipeRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Id",
              "type": "string"
            },
            "revision": {
              "minimum": 1,
              "title": "Revision",
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            }
          },
          "required": [
            "id",
            "revision",
            "sha256"
          ],
          "title": "RecipeRef",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "evaluations": {
          "items": {
            "$ref": "#/$defs/EvaluationView"
          },
          "title": "Evaluations",
          "type": "array"
        },
        "filters": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Filters",
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
          "title": "Order",
          "type": "string"
        },
        "page_size": {
          "maximum": 100,
          "minimum": 1,
          "title": "Page Size",
          "type": "integer"
        },
        "sort": {
          "enum": [
            "updated_at",
            "phase",
            "evaluation_id"
          ],
          "title": "Sort",
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
      "title": "EvaluationPage",
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
