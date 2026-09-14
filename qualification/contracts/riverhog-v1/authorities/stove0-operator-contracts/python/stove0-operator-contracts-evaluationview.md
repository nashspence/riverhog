# stove0_operator_contracts.EvaluationView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationview:cccfaf14d6 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-994c6be83c"></a>`title`: EvaluationView
- <a id="s-47f064b4e4"></a>`description`: Operator projection of a materialized evaluation, not its identity.
- <a id="s-9d43ab102f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-22026ef7b4"></a>`children` | yes | type="array"; items=(#/$defs/EvaluationChildView) |  |
| <a id="s-a9d530d8ea"></a>`definition` | yes | #/$defs/EvaluationDefinition |  |
| <a id="s-8ff988ed5b"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2891a411d2"></a>`format` | no | type="string"; const="stove0-evaluation-view/v1" |  |
| <a id="s-d4ccb27f02"></a>`phase` | yes | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"] |  |
| <a id="s-26307ae6fb"></a>`reviews` | no | type="array"; items=(#/$defs/EvaluationReviewView) |  |
| <a id="s-c725182efd"></a>`revision` | yes | type="integer"; minimum=1 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-a827e0799f"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-b9ce7a1634"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-0d811118d7"></a>`EvaluationChildView` | type="object"; fields=`output`, `state`, `variant_id`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-e3eae98216"></a>`EvaluationDefinition` | type="object"; fields=`common_intent`, `evaluation_id`, `format`, `inputs`, `matrix`, `purpose`, `recipe`; additional keys=`additionalProperties`, `required` |
| <a id="s-feaddbff26"></a>`EvaluationMatrix` | type="object"; fields=`format`, `matrix_sha256`, `variants`; additional keys=`additionalProperties`, `required` |
| <a id="s-90ebe1c97f"></a>`EvaluationReviewView` | type="object"; fields=`note`, `rating`, `updated_at`, `updated_by`, `variant_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-0055b1e45f"></a>`EvaluationVariant` | type="object"; fields=`id`, `parameters`; additional keys=`additionalProperties`, `required` |
| <a id="s-e8be429117"></a>`JsonValue` | empty object |
| <a id="s-0a3cf8b181"></a>`OutputCollectionRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `derivation_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-81eef020ae"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.EvaluationView.exact_identity](stove0-operator-contracts-evaluationview-exact-identity.md)
- [stove0_operator_contracts.EvaluationView.from_record](stove0-operator-contracts-evaluationview-from-record.md)

## Governing policies

- <a id="pa-9f8bca2a83"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4be53e09ebbb6f5c209494909cc5396a9983d3810fffdccd69f7d8426fc9306c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
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
    "signature": "\"(*, format: Literal['stove0-evaluation-view/v1'] = 'stove0-evaluation-view/v1', evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], definition: stove0_protocol.models.EvaluationDefinition, phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=1)], children: tuple[stove0_operator_contracts.EvaluationChildView, ...], reviews: tuple[stove0_operator_contracts.EvaluationReviewView, ...] = ()) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationView",
  "unit": "export"
}
```
