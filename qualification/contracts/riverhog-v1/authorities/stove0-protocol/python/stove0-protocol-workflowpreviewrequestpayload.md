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

- <a id="s-46529a49c6"></a>`type`: `"object"`
- <a id="s-4ecdc1859e"></a>`additionalProperties`: `false`
- <a id="s-d833194f9f"></a>`required`: `["work"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7f788c840c"></a>`format` | no | type="string"; const="stove0-workflow-preview-request/v1"; default="stove0-workflow-preview-request/v1" |  |
| <a id="s-5f427796fc"></a>`work` | yes | [WorkIdentity](#s-8f4e65436f) |  |

##### Definitions

- [BranchWorkBinding](#s-f03ab203f3)
- [CollectionId](#s-dd432c8556)
- [CollectionRootRef](#s-4377f2795b)
- [EvaluationBinding](#s-facf58e54f)
- [JoinWorkBinding](#s-9e064d5ec2)
- [JoinWorkMemberBinding](#s-130e8f22ca)
- [JsonValue](#s-3955bd74a9)
- [RecipeRef](#s-2b3d464bcf)
- [WorkIdentity](#s-8f4e65436f)

##### <a id="s-f03ab203f3"></a>definition `BranchWorkBinding`

- <a id="s-d4ccb46090"></a>`type`: `"object"`
- <a id="s-8b2e4ff530"></a>`additionalProperties`: `false`
- <a id="s-7d0286fd3c"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3808856231"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3fe749abfa"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ca3809c79b"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6e77ffae2a"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-0d22ef3b98"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-dd432c8556"></a>definition `CollectionId`

- <a id="s-423532f415"></a>`type`: `"integer"`
- <a id="s-8d7217494a"></a>`minimum`: `1`

##### <a id="s-4377f2795b"></a>definition `CollectionRootRef`

- <a id="s-c4b1398acd"></a>`type`: `"object"`
- <a id="s-d9c1cee31d"></a>`additionalProperties`: `false`
- <a id="s-c4ca507fca"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7c99e05607"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1d9fced002"></a>`collection_id` | yes | [CollectionId](#s-dd432c8556) |  |
| <a id="s-ca02d4f5b4"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-facf58e54f"></a>definition `EvaluationBinding`

- <a id="s-503402992e"></a>`type`: `"object"`
- <a id="s-0fa38dbf65"></a>`additionalProperties`: `false`
- <a id="s-1bb86b61bb"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-94b916cdf6"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ab7c438a28"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cedcd166de"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-3955bd74a9)) |  |
| <a id="s-d0e7cafb3f"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-9e064d5ec2"></a>definition `JoinWorkBinding`

- <a id="s-8d805ab7e5"></a>`type`: `"object"`
- <a id="s-fbaa2516a2"></a>`additionalProperties`: `false`
- <a id="s-2a8da488ab"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f6518f76ba"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8afac3820d"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-78a4a5a6df"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-130e8f22ca)); minItems=2 |  |
| <a id="s-f32698c8ec"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-130e8f22ca"></a>definition `JoinWorkMemberBinding`

- <a id="s-25b70fac19"></a>`type`: `"object"`
- <a id="s-b1a484197d"></a>`additionalProperties`: `false`
- <a id="s-136a981390"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-11a1b597e0"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-601b559c98"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-38e8b7b97d"></a>`producer_settlement_sha256` | no | anyOf=(type="string"; pattern="^[0-9a-f]{64}$") \| (type="null"); default=null |  |
| <a id="s-6ad02866b5"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3955bd74a9"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-2b3d464bcf"></a>definition `RecipeRef`

- <a id="s-ac143f033d"></a>`type`: `"object"`
- <a id="s-921a07b9ce"></a>`additionalProperties`: `false`
- <a id="s-04f1e47a04"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ec5a953b38"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fa302c6a04"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-eb7fa24d9e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-8f4e65436f"></a>definition `WorkIdentity`

- <a id="s-63dcda98e8"></a>`type`: `"object"`
- <a id="s-d3eca431f2"></a>`additionalProperties`: `false`
- <a id="s-45b40e8a7a"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0e0be19669"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-3955bd74a9)) |  |
| <a id="s-ecbed8eb92"></a>`evaluation` | no | anyOf=([EvaluationBinding](#s-facf58e54f)) \| (type="null"); default=null |  |
| <a id="s-d38c4595ea"></a>`fork_join` | no | anyOf=(discriminator={"mapping":{"branch":"[BranchWorkBinding](#s-f03ab203f3)","join":"[JoinWorkBinding](#s-9e064d5ec2)"},"propertyName":"kind"}; oneOf=([BranchWorkBinding](#s-f03ab203f3)) \| ([JoinWorkBinding](#s-9e064d5ec2))) \| (type="null"); default=null |  |
| <a id="s-93caa4a8dc"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-0b1da9846a"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-4377f2795b)); minItems=1 |  |
| <a id="s-4fed6170d8"></a>`recipe` | yes | [RecipeRef](#s-2b3d464bcf) |  |
| <a id="s-ca55c14df0"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

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

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
