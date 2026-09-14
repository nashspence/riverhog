# stove0_core.EvaluationRecord

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationrecord:51c18da39c -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-14f3254810"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-92e72dcadc"></a>`children` | yes | type="array"; items=(#/$defs/EvaluationChild) |  |
| <a id="s-8a6c30e2c0"></a>`definition` | yes | #/$defs/EvaluationDefinition |  |
| <a id="s-f26c2aa71f"></a>`format` | no | type="string"; const="stove0-evaluation-record/v1" |  |
| <a id="s-6c09420113"></a>`phase` | no | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"] |  |
| <a id="s-8816e049b9"></a>`reviews` | no | type="array"; items=(#/$defs/EvaluationReview) |  |
| <a id="s-1b28e285a9"></a>`revision` | no | type="integer"; minimum=1 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-f1a5fa9dd5"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-70ff7e5b8c"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-d3e952e155"></a>`EvaluationChild` | type="object"; fields=`output`, `state`, `variant_id`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-5039df5a1d"></a>`EvaluationDefinition` | type="object"; fields=`common_intent`, `evaluation_id`, `format`, `inputs`, `matrix`, `purpose`, `recipe`; additional keys=`additionalProperties`, `required` |
| <a id="s-f9f71920c6"></a>`EvaluationMatrix` | type="object"; fields=`format`, `matrix_sha256`, `variants`; additional keys=`additionalProperties`, `required` |
| <a id="s-0949b76fc9"></a>`EvaluationReview` | type="object"; fields=`note`, `rating`, `updated_at`, `updated_by`, `variant_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-aad4dd0548"></a>`EvaluationVariant` | type="object"; fields=`id`, `parameters`; additional keys=`additionalProperties`, `required` |
| <a id="s-c91c28bb44"></a>`JsonValue` | empty object |
| <a id="s-0f5082dc67"></a>`OutputCollectionRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `derivation_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-3790d5b447"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [evaluation_id](stove0-core-evaluationrecord-evaluation-id.md)
- [validate_children](stove0-core-evaluationrecord-validate-children.md)

## Governing policies

- <a id="pa-c9bf4a1f4f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EvaluationRecord`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 31b89d7ae33cf6bdc5935b20e6596c1c36fae40553df200cc91f409ac9d200d5 -->

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
