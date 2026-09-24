# stove0_protocol.CoordinationSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationsettlement:899565cadc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-07baddbb06"></a>
- <a id="s-7a63ff6ded"></a>`distribution`: `stove0-protocol`
- <a id="s-9bc0c3ae17"></a>`module`: `stove0_protocol`
- <a id="s-1f1640076f"></a>`name`: `CoordinationSettlement`
- <a id="s-7c879305db"></a>`unit`: `export`

### Declared structure

- <a id="s-f8e674a532"></a>`kind`: `"class"`
- <a id="s-089f5f2a77"></a>`signature`: `"\"(*, format: Literal['stove0-coordination-settlement/v1'] = 'stove0-coordination-settlement/v1', work: stove0_protocol.models.WorkIdentity, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], children: tuple[stove0_protocol.fork_join.CoordinationChildSettlementRef, ...], contains_external_effects: bool, final_join_settlement_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, collection_result: stove0_protocol.fork_join.CoordinationCollectionResult \| None = None, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-2cf55cc259"></a>

- <a id="s-df263685c6"></a>`type`: `"object"`
- <a id="s-ea0c196f43"></a>`additionalProperties`: `false`
- <a id="s-af0f1ba3d8"></a>`required`: `["work","branch_set_sha256","children","contains_external_effects","settlement_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-931efbb7ae"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bc2661a6f6"></a>`children` | yes | type="array"; items=([CoordinationChildSettlementRef](#s-15c3849639)) |  |
| <a id="s-2baa0230b0"></a>`collection_result` | no | anyOf=[([CoordinationCollectionResult](#s-74d5be0a2c)); (type="null")]; default=null |  |
| <a id="s-f124166c20"></a>`contains_external_effects` | yes | type="boolean" |  |
| <a id="s-4c58376360"></a>`final_join_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-cf158a76e7"></a>`format` | no | type="string"; const="stove0-coordination-settlement/v1"; default="stove0-coordination-settlement/v1" |  |
| <a id="s-ef9f7ebb52"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-23c3d31743"></a>`work` | yes | [WorkIdentity](#s-6d3d9fb2b1) |  |

##### Definitions

- [ArtifactSelectionRef](#s-92ca5a9d9d)
- [BranchWorkBinding](#s-c98157dc98)
- [CollectionId](#s-6a15433853)
- [CollectionRootIdentityRef](#s-b0f1fa8423)
- [CoordinationChildSettlementRef](#s-15c3849639)
- [CoordinationCollectionResult](#s-74d5be0a2c)
- [EvaluationBinding](#s-fc51dad3c9)
- [JoinWorkBinding](#s-b6c0046749)
- [JoinWorkMemberBinding](#s-429d9c9e8b)
- [JsonValue](#s-73f7ec6527)
- [NonnegativeDecimal](#s-3348eae9e9)
- [RecipeIdentityRef](#s-7dede690fe)
- [WorkIdentity](#s-6d3d9fb2b1)

##### <a id="s-92ca5a9d9d"></a>definition `ArtifactSelectionRef`

- <a id="s-27f7962141"></a>`type`: `"object"`
- <a id="s-c493a54b7c"></a>`additionalProperties`: `false`
- <a id="s-0ab0450b65"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5f1faf43cc"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-203294cb6f"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2b1b4c4f34"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-3348eae9e9); ge=0 |  |

##### <a id="s-c98157dc98"></a>definition `BranchWorkBinding`

- <a id="s-dad9e32297"></a>`type`: `"object"`
- <a id="s-559e9fcdef"></a>`additionalProperties`: `false`
- <a id="s-db6445888b"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f81b58a159"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3a233c809e"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3093cf91aa"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a24192d169"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-0a3b4904ee"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-6a15433853"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-f1ec6ce84e"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-8bb6d8f338"></a>2 | not=(const="0") |

##### <a id="s-b0f1fa8423"></a>definition `CollectionRootIdentityRef`

- <a id="s-541b592a4d"></a>`type`: `"object"`
- <a id="s-f981d3c81c"></a>`additionalProperties`: `false`
- <a id="s-f8c8200c3f"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3736a0fc21"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-eb06460fa2"></a>`collection_id` | yes | [CollectionId](#s-6a15433853) |  |
| <a id="s-00dc1aaf87"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-15c3849639"></a>definition `CoordinationChildSettlementRef`

- <a id="s-8f2a35eae9"></a>`type`: `"object"`
- <a id="s-355ad098dd"></a>`additionalProperties`: `false`
- <a id="s-29b88875b3"></a>`required`: `["branch_id","kind","settlement_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5b1075e6b3"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-63d542adfa"></a>`kind` | yes | type="string"; enum=["collection","external-effect","coordination"] |  |
| <a id="s-d53a1e04e8"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-74d5be0a2c"></a>definition `CoordinationCollectionResult`

- <a id="s-db7b6134d7"></a>`type`: `"object"`
- <a id="s-96cde77c08"></a>`additionalProperties`: `false`
- <a id="s-358e402fe7"></a>`required`: `["producer_work_id","join_settlement_sha256","derivation_sha256","output_collection","output_selection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4038393a7f"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d042a6b105"></a>`join_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c5bbf9ce9c"></a>`output_collection` | yes | [CollectionRootIdentityRef](#s-b0f1fa8423) |  |
| <a id="s-fac241b3ef"></a>`output_selection` | yes | [ArtifactSelectionRef](#s-92ca5a9d9d) |  |
| <a id="s-a314aa9441"></a>`producer_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-fc51dad3c9"></a>definition `EvaluationBinding`

- <a id="s-7488236949"></a>`type`: `"object"`
- <a id="s-119d88171f"></a>`additionalProperties`: `false`
- <a id="s-a07b00f08a"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f91fd4f3bb"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6017a3b57f"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-057afc2bf5"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-73f7ec6527)) |  |
| <a id="s-9795b428c7"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-b6c0046749"></a>definition `JoinWorkBinding`

- <a id="s-0aa4548e37"></a>`type`: `"object"`
- <a id="s-ad0c382a47"></a>`additionalProperties`: `false`
- <a id="s-41cb0d8218"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6c6e8a401a"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3d59394fe9"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-f70452f72e"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-429d9c9e8b)); minItems=2 |  |
| <a id="s-676306f9e5"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-429d9c9e8b"></a>definition `JoinWorkMemberBinding`

- <a id="s-37c33bd033"></a>`type`: `"object"`
- <a id="s-a412f064c5"></a>`additionalProperties`: `false`
- <a id="s-a597bcb8e3"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-28b48c62fb"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-79d96b9983"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-84eff4f801"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-7643a83044"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-73f7ec6527"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-3348eae9e9"></a>definition `NonnegativeDecimal`

- <a id="s-9a888208e0"></a>`type`: `"string"`
- <a id="s-2ce286f521"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-7dede690fe"></a>definition `RecipeIdentityRef`

- <a id="s-8284d13923"></a>`type`: `"object"`
- <a id="s-e125e3d4e2"></a>`additionalProperties`: `false`
- <a id="s-a0f1760d7e"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b244b1409e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-6d7bace128"></a>`revision` | yes | [NonnegativeDecimal](#s-3348eae9e9); ge=1 |  |
| <a id="s-28a342e807"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-6d3d9fb2b1"></a>definition `WorkIdentity`

- <a id="s-388e161e5c"></a>`type`: `"object"`
- <a id="s-1828526af7"></a>`additionalProperties`: `false`
- <a id="s-35f07a5e72"></a>`required`: `["recipe","inputs","work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-941d6ccd28"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-73f7ec6527)) |  |
| <a id="s-8ab0296b24"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-fc51dad3c9)); (type="null")]; default=null |  |
| <a id="s-54abbd5097"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-c98157dc98)); ([JoinWorkBinding](#s-b6c0046749))]); (type="null")]; default=null |  |
| <a id="s-fa750f01f0"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-133d5e842e"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-b0f1fa8423)); minItems=1 |  |
| <a id="s-76244f13e3"></a>`recipe` | yes | [RecipeIdentityRef](#s-7dede690fe) |  |
| <a id="s-08cd8d312f"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_children](stove0-protocol-coordinationsettlement-canonical-children.md)
- [seal](stove0-protocol-coordinationsettlement-seal.md)
- [verify_contract](stove0-protocol-coordinationsettlement-verify-contract.md)

## Governing policies

- <a id="pa-fa9286ce2f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationSettlement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c2171bfdd4e353c164c25d567022256f1515ec7e2ca5f3c2d008ee5acb3524eb -->

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
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 0
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
        "CoordinationChildSettlementRef": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "kind": {
              "enum": [
                "collection",
                "external-effect",
                "coordination"
              ],
              "type": "string"
            },
            "settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "branch_id",
            "kind",
            "settlement_sha256"
          ],
          "type": "object"
        },
        "CoordinationCollectionResult": {
          "additionalProperties": false,
          "properties": {
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "join_settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "output_collection": {
              "$ref": "#/$defs/CollectionRootIdentityRef"
            },
            "output_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "producer_work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "producer_work_id",
            "join_settlement_sha256",
            "derivation_sha256",
            "output_collection",
            "output_selection"
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
        "branch_set_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "children": {
          "items": {
            "$ref": "#/$defs/CoordinationChildSettlementRef"
          },
          "type": "array"
        },
        "collection_result": {
          "anyOf": [
            {
              "$ref": "#/$defs/CoordinationCollectionResult"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "contains_external_effects": {
          "type": "boolean"
        },
        "final_join_settlement_sha256": {
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
        "format": {
          "const": "stove0-coordination-settlement/v1",
          "default": "stove0-coordination-settlement/v1",
          "type": "string"
        },
        "settlement_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "work": {
          "$ref": "#/$defs/WorkIdentity"
        }
      },
      "required": [
        "work",
        "branch_set_sha256",
        "children",
        "contains_external_effects",
        "settlement_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-coordination-settlement/v1'] = 'stove0-coordination-settlement/v1', work: stove0_protocol.models.WorkIdentity, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], children: tuple[stove0_protocol.fork_join.CoordinationChildSettlementRef, ...], contains_external_effects: bool, final_join_settlement_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, collection_result: stove0_protocol.fork_join.CoordinationCollectionResult | None = None, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "CoordinationSettlement",
  "unit": "export"
}
```

</details>
