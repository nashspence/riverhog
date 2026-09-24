# stove0_protocol.WorkflowPreviewRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewrequest:31e4186516 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-08bd206d1b"></a>
- <a id="s-658f136d9e"></a>`distribution`: `stove0-protocol`
- <a id="s-f10178efbf"></a>`module`: `stove0_protocol`
- <a id="s-5af61f8738"></a>`name`: `WorkflowPreviewRequest`
- <a id="s-294e1eeecf"></a>`unit`: `export`

### Declared structure

- <a id="s-2b427c210e"></a>`kind`: `"class"`
- <a id="s-8e91e48642"></a>`signature`: `"\"(*, format: Literal['stove0-workflow-preview-request/v1'] = 'stove0-workflow-preview-request/v1', work: stove0_protocol.models.WorkIdentity, preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-4d67785717"></a>

- <a id="s-8f2083971a"></a>`type`: `"object"`
- <a id="s-7b967aa87b"></a>`additionalProperties`: `false`
- <a id="s-f90c8f2fb5"></a>`required`: `["work","preview_id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1c050c94d5"></a>`format` | no | type="string"; const="stove0-workflow-preview-request/v1"; default="stove0-workflow-preview-request/v1" |  |
| <a id="s-5d9f5f80d5"></a>`preview_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-282cf0f044"></a>`work` | yes | [WorkIdentity](#s-ade104b3c3) |  |

##### Definitions

- [BranchWorkBinding](#s-b55a738295)
- [CollectionId](#s-4aca3f2f1b)
- [CollectionRootIdentityRef](#s-d8864aa9a0)
- [EvaluationBinding](#s-294c0b0641)
- [JoinWorkBinding](#s-fe6b314496)
- [JoinWorkMemberBinding](#s-5a9ef46cbf)
- [JsonValue](#s-722fca32e8)
- [NonnegativeDecimal](#s-554e1a783c)
- [RecipeIdentityRef](#s-8a39867277)
- [WorkIdentity](#s-ade104b3c3)

##### <a id="s-b55a738295"></a>definition `BranchWorkBinding`

- <a id="s-689ac50492"></a>`type`: `"object"`
- <a id="s-b463d48842"></a>`additionalProperties`: `false`
- <a id="s-b33509696b"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e1998a2d82"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f8a3be30f7"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ae2a44c018"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6216cd6e4f"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-c3c97ff642"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-4aca3f2f1b"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-0e5a009d15"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-d6d21c9085"></a>2 | not=(const="0") |

##### <a id="s-d8864aa9a0"></a>definition `CollectionRootIdentityRef`

- <a id="s-09acdef8f2"></a>`type`: `"object"`
- <a id="s-d5cecbdf0a"></a>`additionalProperties`: `false`
- <a id="s-57852ff8ed"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c52f6fdcd5"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-43b19d5e0b"></a>`collection_id` | yes | [CollectionId](#s-4aca3f2f1b) |  |
| <a id="s-01215a791c"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-294c0b0641"></a>definition `EvaluationBinding`

- <a id="s-e7614f3651"></a>`type`: `"object"`
- <a id="s-f6cf1acca8"></a>`additionalProperties`: `false`
- <a id="s-d5fbeefd53"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-37a025cf33"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5662a7bfa2"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d72994c605"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-722fca32e8)) |  |
| <a id="s-7d4e92e0b4"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-fe6b314496"></a>definition `JoinWorkBinding`

- <a id="s-53ef423a82"></a>`type`: `"object"`
- <a id="s-89a7de6051"></a>`additionalProperties`: `false`
- <a id="s-8e5bc97740"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d1a49b29d3"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7127d30e4f"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-57a3c78bf6"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-5a9ef46cbf)); minItems=2 |  |
| <a id="s-ee2f195272"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5a9ef46cbf"></a>definition `JoinWorkMemberBinding`

- <a id="s-ed3344745e"></a>`type`: `"object"`
- <a id="s-40d67f9c58"></a>`additionalProperties`: `false`
- <a id="s-323d7904e7"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a047784521"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a3dd3e50dc"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7cd3fab49d"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-bb47287486"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-722fca32e8"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-554e1a783c"></a>definition `NonnegativeDecimal`

- <a id="s-ea2375514e"></a>`type`: `"string"`
- <a id="s-667e5008a1"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-8a39867277"></a>definition `RecipeIdentityRef`

- <a id="s-1c49af9bcd"></a>`type`: `"object"`
- <a id="s-eb3d45ed8e"></a>`additionalProperties`: `false`
- <a id="s-5443a8a6f4"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b86e499ffa"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-bc0273731a"></a>`revision` | yes | [NonnegativeDecimal](#s-554e1a783c); ge=1 |  |
| <a id="s-f481f8f991"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ade104b3c3"></a>definition `WorkIdentity`

- <a id="s-363b907c3b"></a>`type`: `"object"`
- <a id="s-ccaf6861e3"></a>`additionalProperties`: `false`
- <a id="s-706708816a"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6a90de9adc"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-722fca32e8)) |  |
| <a id="s-44f59dac8f"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-294c0b0641)); (type="null")]; default=null |  |
| <a id="s-1aedde3f20"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-b55a738295)); ([JoinWorkBinding](#s-fe6b314496))]); (type="null")]; default=null |  |
| <a id="s-59f13be967"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-2fee1762ba"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-d8864aa9a0)); minItems=1 |  |
| <a id="s-26a7af3100"></a>`recipe` | yes | [RecipeIdentityRef](#s-8a39867277) |  |
| <a id="s-d59332f9a7"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [seal](stove0-protocol-workflowpreviewrequest-seal.md)
- [verify_digest](stove0-protocol-workflowpreviewrequest-verify-digest.md)

## Governing policies

- <a id="pa-8259362854"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9ab8daa09e9525aeefd4867adbf83924394bdb33f8cb2f604fe072906c1333dc -->

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
        "preview_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "work": {
          "$ref": "#/$defs/WorkIdentity"
        }
      },
      "required": [
        "work",
        "preview_id"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-workflow-preview-request/v1'] = 'stove0-workflow-preview-request/v1', work: stove0_protocol.models.WorkIdentity, preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPreviewRequest",
  "unit": "export"
}
```

</details>
