# stove0_protocol.WorkPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workpayload:fa6cad1292 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d1d5916d09"></a>
- <a id="s-a0010a3270"></a>`distribution`: `stove0-protocol`
- <a id="s-4ba5384cb7"></a>`module`: `stove0_protocol`
- <a id="s-d71432a01d"></a>`name`: `WorkPayload`
- <a id="s-c52c1c6d11"></a>`unit`: `export`

### Declared structure

- <a id="s-610afc0fec"></a>`kind`: `"class"`
- <a id="s-948950d129"></a>`signature`: `"\"(*, format: Literal['stove0-work/v1'] = 'stove0-work/v1', recipe: stove0_protocol.models.RecipeRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, evaluation: stove0_protocol.models.EvaluationBinding \| None = None, fork_join: Optional[Annotated[stove0_protocol.models.BranchWorkBinding \| stove0_protocol.models.JoinWorkBinding, FieldInfo(annotation=NoneType, required=True, discriminator='kind')]] = None) -> None\""`

#### Validated model schema

<a id="s-68ae3029d6"></a>

- <a id="s-b158f209cb"></a>`type`: `"object"`
- <a id="s-4fe2d7b10b"></a>`additionalProperties`: `false`
- <a id="s-6fc60cc39f"></a>`required`: `["recipe","inputs"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9d2841908e"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-ad2c6770c7)) |  |
| <a id="s-4272ceb064"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-ed54a486b1)); (type="null")]; default=null |  |
| <a id="s-9d54a2d47b"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-c6a1e077ed)); ([JoinWorkBinding](#s-e1f4afc65b))]); (type="null")]; default=null |  |
| <a id="s-e89ea0fa5c"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-07544b90a3"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-d63064b523)); minItems=1 |  |
| <a id="s-03b0aa019f"></a>`recipe` | yes | [RecipeRef](#s-205ecdb9f4) |  |

##### Definitions

- [BranchWorkBinding](#s-c6a1e077ed)
- [CollectionId](#s-6196ada8e8)
- [CollectionRootRef](#s-d63064b523)
- [EvaluationBinding](#s-ed54a486b1)
- [JoinWorkBinding](#s-e1f4afc65b)
- [JoinWorkMemberBinding](#s-f8d74e7c3f)
- [JsonValue](#s-ad2c6770c7)
- [RecipeRef](#s-205ecdb9f4)

##### <a id="s-c6a1e077ed"></a>definition `BranchWorkBinding`

- <a id="s-c92332a614"></a>`type`: `"object"`
- <a id="s-0fe0a3233c"></a>`additionalProperties`: `false`
- <a id="s-f402796c02"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-591c34efa2"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d94eba35f5"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-886a753b3f"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-88793e1104"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-6be715ec48"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-6196ada8e8"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-5aeae6e9be"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-7736091a1a"></a>2 | not=(const="0") |

##### <a id="s-d63064b523"></a>definition `CollectionRootRef`

- <a id="s-c4a55f67b9"></a>`type`: `"object"`
- <a id="s-84d679c431"></a>`additionalProperties`: `false`
- <a id="s-29d174e10c"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0770abf046"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1901ed9469"></a>`collection_id` | yes | [CollectionId](#s-6196ada8e8) |  |
| <a id="s-ecd587b812"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ed54a486b1"></a>definition `EvaluationBinding`

- <a id="s-2648837d60"></a>`type`: `"object"`
- <a id="s-60692192ef"></a>`additionalProperties`: `false`
- <a id="s-0120ec18ce"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-845231968d"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-60dcae941d"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-986fa1c430"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-ad2c6770c7)) |  |
| <a id="s-d436da950a"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-e1f4afc65b"></a>definition `JoinWorkBinding`

- <a id="s-381e97d22d"></a>`type`: `"object"`
- <a id="s-057a169978"></a>`additionalProperties`: `false`
- <a id="s-bd148bacdd"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a79ea1ca45"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-227d45b673"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-7612884469"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-f8d74e7c3f)); minItems=2 |  |
| <a id="s-8e5dbffe68"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f8d74e7c3f"></a>definition `JoinWorkMemberBinding`

- <a id="s-5f06a2a1ab"></a>`type`: `"object"`
- <a id="s-b72e7d680a"></a>`additionalProperties`: `false`
- <a id="s-713cf24e58"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-095684f4e3"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8c5064d3fb"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ca364d78e1"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-ad48e114bd"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ad2c6770c7"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-205ecdb9f4"></a>definition `RecipeRef`

- <a id="s-2990fe80d2"></a>`type`: `"object"`
- <a id="s-79016cc178"></a>`additionalProperties`: `false`
- <a id="s-175bd47641"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-618eda3f10"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5669473844"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-c1c2000f95"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_inputs](stove0-protocol-workpayload-canonical-inputs.md)

## Governing policies

- <a id="pa-22c50e65a6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8efe8f3af8842fa267a33651f69f7c87b58b833876ad7bd4c12529b995502f5 -->

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
        }
      },
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
        }
      },
      "required": [
        "recipe",
        "inputs"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-work/v1'] = 'stove0-work/v1', recipe: stove0_protocol.models.RecipeRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, evaluation: stove0_protocol.models.EvaluationBinding | None = None, fork_join: Optional[Annotated[stove0_protocol.models.BranchWorkBinding | stove0_protocol.models.JoinWorkBinding, FieldInfo(annotation=NoneType, required=True, discriminator='kind')]] = None) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkPayload",
  "unit": "export"
}
```

</details>
