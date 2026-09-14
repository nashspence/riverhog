# stove0_protocol.WorkflowPreviewRequestPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewrequestpayload:b32657871e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-37a175ba7b"></a>
- <a id="s-52fcf38390"></a>`distribution`: `stove0-protocol`
- <a id="s-d14cc8b3a6"></a>`module`: `stove0_protocol`
- <a id="s-fffd2c399d"></a>`name`: `WorkflowPreviewRequestPayload`
- <a id="s-eb2797644b"></a>`unit`: `export`

### Declared structure

- <a id="s-cc12c16e9d"></a>`kind`: `"class"`
- <a id="s-1bea040c63"></a>`signature`: `"\"(*, format: Literal['stove0-workflow-preview-request/v1'] = 'stove0-workflow-preview-request/v1', work: stove0_protocol.models.WorkIdentity) -> None\""`

#### Validated model schema

<a id="s-2de1bd2ad9"></a>
- <a id="s-46529a49c6"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7f788c840c"></a>`format` | no | type="string"; const="stove0-workflow-preview-request/v1" |  |
| <a id="s-5f427796fc"></a>`work` | yes | #/$defs/WorkIdentity |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-f03ab203f3"></a>`BranchWorkBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `decision_sha256`, `kind`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-dd432c8556"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-4377f2795b"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-facf58e54f"></a>`EvaluationBinding` | type="object"; fields=`evaluation_id`, `matrix_sha256`, `parameters`, `variant_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-9e064d5ec2"></a>`JoinWorkBinding` | type="object"; fields=`branch_set_sha256`, `kind`, `members`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-130e8f22ca"></a>`JoinWorkMemberBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `producer_settlement_sha256`, `settlement_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-3955bd74a9"></a>`JsonValue` | empty object |
| <a id="s-2b3d464bcf"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-8f4e65436f"></a>`WorkIdentity` | type="object"; fields=`effective_intent`, `evaluation`, `fork_join`, `format`, `inputs`, `recipe`, `work_id`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-3f89f6a8ce"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewRequestPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f9aace2c98e98c4b2b4494c9b5fb86542403a35f49df464e83014307a7adec2a -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "BranchWorkBinding": {
          "additionalProperties": false,
          "properties": {
            "artifact_selection_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "decision_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "kind": {
              "const": "branch",
              "default": "branch",
              "type": "string"
            },
            "parent_work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "parent_work_id",
            "branch_id",
            "decision_sha256",
            "artifact_selection_sha256"
          ],
          "type": "object"
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
        "EvaluationBinding": {
          "additionalProperties": false,
          "properties": {
            "evaluation_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "matrix_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "parameters": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "variant_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "evaluation_id",
            "matrix_sha256",
            "variant_id"
          ],
          "type": "object"
        },
        "JoinWorkBinding": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "kind": {
              "const": "join",
              "default": "join",
              "type": "string"
            },
            "members": {
              "items": {
                "$ref": "#/$defs/JoinWorkMemberBinding"
              },
              "minItems": 2,
              "type": "array"
            },
            "parent_work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "parent_work_id",
            "branch_set_sha256",
            "members"
          ],
          "type": "object"
        },
        "JoinWorkMemberBinding": {
          "additionalProperties": false,
          "properties": {
            "artifact_selection_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "producer_settlement_sha256": {
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
            "settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "branch_id",
            "settlement_sha256",
            "artifact_selection_sha256"
          ],
          "type": "object"
        },
        "JsonValue": {},
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
        },
        "WorkIdentity": {
          "additionalProperties": false,
          "properties": {
            "effective_intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "evaluation": {
              "anyOf": [
                {
                  "$ref": "#/$defs/EvaluationBinding"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "fork_join": {
              "anyOf": [
                {
                  "discriminator": {
                    "mapping": {
                      "branch": "#/$defs/BranchWorkBinding",
                      "join": "#/$defs/JoinWorkBinding"
                    },
                    "propertyName": "kind"
                  },
                  "oneOf": [
                    {
                      "$ref": "#/$defs/BranchWorkBinding"
                    },
                    {
                      "$ref": "#/$defs/JoinWorkBinding"
                    }
                  ]
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "format": {
              "const": "stove0-work/v1",
              "default": "stove0-work/v1",
              "type": "string"
            },
            "inputs": {
              "items": {
                "$ref": "#/$defs/CollectionRootRef"
              },
              "minItems": 1,
              "type": "array"
            },
            "recipe": {
              "$ref": "#/$defs/RecipeRef"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "recipe",
            "inputs",
            "work_id"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-workflow-preview-request/v1",
          "default": "stove0-workflow-preview-request/v1",
          "type": "string"
        },
        "work": {
          "$ref": "#/$defs/WorkIdentity"
        }
      },
      "required": [
        "work"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-workflow-preview-request/v1'] = 'stove0-workflow-preview-request/v1', work: stove0_protocol.models.WorkIdentity) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPreviewRequestPayload",
  "unit": "export"
}
```
