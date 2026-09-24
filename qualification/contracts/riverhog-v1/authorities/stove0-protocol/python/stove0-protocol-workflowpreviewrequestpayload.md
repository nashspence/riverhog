# stove0_protocol.WorkflowPreviewRequestPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewrequestpayload:b32657871e -->

Exact externally visible contract owned by this contract element.

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
- [CollectionRootIdentityRef](#s-3f1fe104ca)
- [EvaluationBinding](#s-facf58e54f)
- [JoinWorkBinding](#s-9e064d5ec2)
- [JoinWorkMemberBinding](#s-130e8f22ca)
- [JsonValue](#s-3955bd74a9)
- [NonnegativeDecimal](#s-ef87653d0c)
- [RecipeIdentityRef](#s-25bd119ad8)
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


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-9cc8e1a084"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-bdf7456d43"></a>2 | not=(const="0") |

##### <a id="s-3f1fe104ca"></a>definition `CollectionRootIdentityRef`

- <a id="s-7c49cf045e"></a>`type`: `"object"`
- <a id="s-5a8f32d9ac"></a>`additionalProperties`: `false`
- <a id="s-ecc7b3229c"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f3f8a3cf14"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-eba25b7b81"></a>`collection_id` | yes | [CollectionId](#s-dd432c8556) |  |
| <a id="s-b597e819f2"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

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
| <a id="s-38e8b7b97d"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-6ad02866b5"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-3955bd74a9"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-ef87653d0c"></a>definition `NonnegativeDecimal`

- <a id="s-50c3836e6b"></a>`type`: `"string"`
- <a id="s-668a94ee66"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-25bd119ad8"></a>definition `RecipeIdentityRef`

- <a id="s-a149fb3c59"></a>`type`: `"object"`
- <a id="s-43c1315418"></a>`additionalProperties`: `false`
- <a id="s-eb060a44f7"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-012f77d3fa"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-70958d6afc"></a>`revision` | yes | [NonnegativeDecimal](#s-ef87653d0c); ge=1 |  |
| <a id="s-1ada44b830"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-8f4e65436f"></a>definition `WorkIdentity`

- <a id="s-63dcda98e8"></a>`type`: `"object"`
- <a id="s-d3eca431f2"></a>`additionalProperties`: `false`
- <a id="s-45b40e8a7a"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0e0be19669"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-3955bd74a9)) |  |
| <a id="s-ecbed8eb92"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-facf58e54f)); (type="null")]; default=null |  |
| <a id="s-d38c4595ea"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-f03ab203f3)); ([JoinWorkBinding](#s-9e064d5ec2))]); (type="null")]; default=null |  |
| <a id="s-93caa4a8dc"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-0b1da9846a"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-3f1fe104ca)); minItems=1 |  |
| <a id="s-4fed6170d8"></a>`recipe` | yes | [RecipeIdentityRef](#s-25bd119ad8) |  |
| <a id="s-ca55c14df0"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Governing policies

- <a id="pa-3f89f6a8ce"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewRequestPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 05f453d90f04fcd25a6107c1377af282962684dc7e559e4316b7b82cd6600341 -->

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
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        },
        "RecipeIdentityRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "revision": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
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
                "$ref": "#/$defs/CollectionRootIdentityRef"
              },
              "minItems": 1,
              "type": "array"
            },
            "recipe": {
              "$ref": "#/$defs/RecipeIdentityRef"
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
