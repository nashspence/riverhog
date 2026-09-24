# stove0_protocol.CoordinationBranchPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationbranchplan:f23e6c90ab -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc1bebe5d6"></a>
- <a id="s-9b962a3eb3"></a>`distribution`: `stove0-protocol`
- <a id="s-2fd891f6e0"></a>`module`: `stove0_protocol`
- <a id="s-6a5a490ffa"></a>`name`: `CoordinationBranchPlan`
- <a id="s-d4f539a06f"></a>`unit`: `export`

### Declared structure

- <a id="s-869148eb7c"></a>`kind`: `"class"`
- <a id="s-c200a14bc6"></a>`signature`: `"\"(*, kind: Literal['coordination'] = 'coordination', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], artifact_selection: stove0_protocol.fork_join.ArtifactSelectionRef, work: stove0_protocol.models.WorkIdentity, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-9442ae13d1"></a>

- <a id="s-5de6dc67a5"></a>`type`: `"object"`
- <a id="s-23c478f981"></a>`additionalProperties`: `false`
- <a id="s-234c78f788"></a>`required`: `["branch_id","artifact_selection","work","branch_set_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-812aa243a6"></a>`artifact_selection` | yes | [ArtifactSelectionRef](#s-b33c09e2a3) |  |
| <a id="s-b55c7b6037"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3851095f44"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-df320202b1"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-b4ad8e3b45"></a>`work` | yes | [WorkIdentity](#s-53618e1cd2) |  |

##### Definitions

- [ArtifactSelectionRef](#s-b33c09e2a3)
- [BranchWorkBinding](#s-82983e92f5)
- [CollectionId](#s-c0da401907)
- [CollectionRootIdentityRef](#s-bd6947b82c)
- [EvaluationBinding](#s-d3bc9a7c09)
- [JoinWorkBinding](#s-5f78a1f478)
- [JoinWorkMemberBinding](#s-50f9632db8)
- [JsonValue](#s-0537c3bf06)
- [NonnegativeDecimal](#s-2c0ca5351c)
- [RecipeIdentityRef](#s-b761c9a961)
- [WorkIdentity](#s-53618e1cd2)

##### <a id="s-b33c09e2a3"></a>definition `ArtifactSelectionRef`

- <a id="s-54535fc9c6"></a>`type`: `"object"`
- <a id="s-ebe6580fd7"></a>`additionalProperties`: `false`
- <a id="s-248b035e3c"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-89d087c472"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-77b8189be9"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-967f011017"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-82983e92f5"></a>definition `BranchWorkBinding`

- <a id="s-5edd0e6db1"></a>`type`: `"object"`
- <a id="s-890658f093"></a>`additionalProperties`: `false`
- <a id="s-fb02f2bb1c"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8e4f761ffc"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f03858d6c9"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d391090fc8"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d4e467d60c"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-70b11a3c65"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c0da401907"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-c9ca3d8543"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-01a9daa500"></a>2 | not=(const="0") |

##### <a id="s-bd6947b82c"></a>definition `CollectionRootIdentityRef`

- <a id="s-f878693a50"></a>`type`: `"object"`
- <a id="s-464cadbe4d"></a>`additionalProperties`: `false`
- <a id="s-1cdf21924d"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d83b40674b"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c180cb4c36"></a>`collection_id` | yes | [CollectionId](#s-c0da401907) |  |
| <a id="s-1e899ef920"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-d3bc9a7c09"></a>definition `EvaluationBinding`

- <a id="s-394936a60c"></a>`type`: `"object"`
- <a id="s-1b61f57834"></a>`additionalProperties`: `false`
- <a id="s-cbd3e1a165"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-40630deff9"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-92c7ec06ed"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1bbd67b43c"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-0537c3bf06)) |  |
| <a id="s-47218c8246"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-5f78a1f478"></a>definition `JoinWorkBinding`

- <a id="s-4939c44b7b"></a>`type`: `"object"`
- <a id="s-83fb95438a"></a>`additionalProperties`: `false`
- <a id="s-86c0a62143"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-83da661a7b"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-67e6c270a8"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-365101f7dc"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-50f9632db8)); minItems=2 |  |
| <a id="s-bdcb0abd79"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-50f9632db8"></a>definition `JoinWorkMemberBinding`

- <a id="s-f491b45bb0"></a>`type`: `"object"`
- <a id="s-71ecaee7c8"></a>`additionalProperties`: `false`
- <a id="s-5a36f37272"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0966922652"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0b4ac44c89"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4d2906867a"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-a15975e671"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0537c3bf06"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-2c0ca5351c"></a>definition `NonnegativeDecimal`

- <a id="s-08e3951762"></a>`type`: `"string"`
- <a id="s-ae588412c2"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-b761c9a961"></a>definition `RecipeIdentityRef`

- <a id="s-2f5ec87b02"></a>`type`: `"object"`
- <a id="s-43fc509d18"></a>`additionalProperties`: `false`
- <a id="s-fb1142dc7a"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6a8e926aee"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-680018724d"></a>`revision` | yes | [NonnegativeDecimal](#s-2c0ca5351c); ge=1 |  |
| <a id="s-08195f1d6f"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-53618e1cd2"></a>definition `WorkIdentity`

- <a id="s-edd3145e18"></a>`type`: `"object"`
- <a id="s-b79b465c66"></a>`additionalProperties`: `false`
- <a id="s-9053bc6ac4"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-905407c860"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-0537c3bf06)) |  |
| <a id="s-fda109ffae"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-d3bc9a7c09)); (type="null")]; default=null |  |
| <a id="s-cb3bc75fb5"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-82983e92f5)); ([JoinWorkBinding](#s-5f78a1f478))]); (type="null")]; default=null |  |
| <a id="s-df5eac0440"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-3f0333a677"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-bd6947b82c)); minItems=1 |  |
| <a id="s-0c0a9cadf8"></a>`recipe` | yes | [RecipeIdentityRef](#s-b761c9a961) |  |
| <a id="s-255414fa56"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [bind_child_work](stove0-protocol-coordinationbranchplan-bind-child-work.md)
- [build_work](stove0-protocol-coordinationbranchplan-build-work.md)
- [build](stove0-protocol-coordinationbranchplan-build.md)

## Governing policies

- <a id="pa-571e354fc0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationBranchPlan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f63e5743205e81f6e4b4c516b5075ee3aa1a9556bbf98fffd13ed97b6c44c756 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactSelectionRef": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
            },
            "selection_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "selection_sha256",
            "artifact_count",
            "total_bytes"
          ],
          "type": "object"
        },
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
        "artifact_selection": {
          "$ref": "#/$defs/ArtifactSelectionRef"
        },
        "branch_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "branch_set_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "kind": {
          "const": "coordination",
          "default": "coordination",
          "type": "string"
        },
        "work": {
          "$ref": "#/$defs/WorkIdentity"
        }
      },
      "required": [
        "branch_id",
        "artifact_selection",
        "work",
        "branch_set_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, kind: Literal['coordination'] = 'coordination', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], artifact_selection: stove0_protocol.fork_join.ArtifactSelectionRef, work: stove0_protocol.models.WorkIdentity, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "CoordinationBranchPlan",
  "unit": "export"
}
```

</details>
