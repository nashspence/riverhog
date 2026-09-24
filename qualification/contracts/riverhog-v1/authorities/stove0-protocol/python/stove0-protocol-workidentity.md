# stove0_protocol.WorkIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workidentity:41554d3c31 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a9355fab3f"></a>
- <a id="s-21e699da7a"></a>`distribution`: `stove0-protocol`
- <a id="s-f2c1899532"></a>`module`: `stove0_protocol`
- <a id="s-09cef259be"></a>`name`: `WorkIdentity`
- <a id="s-6a6f9ccdaf"></a>`unit`: `export`

### Declared structure

- <a id="s-edadd6387d"></a>`kind`: `"class"`
- <a id="s-f690998967"></a>`signature`: `"\"(*, format: Literal['stove0-work/v1'] = 'stove0-work/v1', recipe: stove0_protocol.models.RecipeIdentityRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootIdentityRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, evaluation: stove0_protocol.models.EvaluationBinding \| None = None, fork_join: Optional[Annotated[stove0_protocol.models.BranchWorkBinding \| stove0_protocol.models.JoinWorkBinding, FieldInfo(annotation=NoneType, required=True, discriminator='kind')]] = None, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-72c2276867"></a>

- <a id="s-f458d0a678"></a>`type`: `"object"`
- <a id="s-3b72662978"></a>`additionalProperties`: `false`
- <a id="s-df97c1ce2e"></a>`required`: `["recipe","inputs","work_id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fd81682d93"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-6239d9390d)) |  |
| <a id="s-c03e608b1b"></a>`evaluation` | no | anyOf=[([EvaluationBinding](#s-5c44a90899)); (type="null")]; default=null |  |
| <a id="s-d3499ce402"></a>`fork_join` | no | anyOf=[(discriminator={"mapping":{"branch":"#/$defs/BranchWorkBinding","join":"#/$defs/JoinWorkBinding"},"propertyName":"kind"}; oneOf=[([BranchWorkBinding](#s-42e530e4b6)); ([JoinWorkBinding](#s-8da12a9a78))]); (type="null")]; default=null |  |
| <a id="s-023f2e2a82"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-e3be119012"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](#s-f45030d4e6)); minItems=1 |  |
| <a id="s-bcb81b9438"></a>`recipe` | yes | [RecipeIdentityRef](#s-0868d9822c) |  |
| <a id="s-973dbaa04f"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [BranchWorkBinding](#s-42e530e4b6)
- [CollectionId](#s-7f283b7e62)
- [CollectionRootIdentityRef](#s-f45030d4e6)
- [EvaluationBinding](#s-5c44a90899)
- [JoinWorkBinding](#s-8da12a9a78)
- [JoinWorkMemberBinding](#s-bb9e7aba49)
- [JsonValue](#s-6239d9390d)
- [RecipeIdentityRef](#s-0868d9822c)

##### <a id="s-42e530e4b6"></a>definition `BranchWorkBinding`

- <a id="s-fcb31681ac"></a>`type`: `"object"`
- <a id="s-c501601a9d"></a>`additionalProperties`: `false`
- <a id="s-de038b47a4"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a551b9f2d"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a3d6b61926"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-570d6551e6"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b1a8223d79"></a>`kind` | no | type="string"; const="branch"; default="branch" |  |
| <a id="s-9d31799368"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-7f283b7e62"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-11fcd0d645"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-ca448000aa"></a>2 | not=(const="0") |

##### <a id="s-f45030d4e6"></a>definition `CollectionRootIdentityRef`

- <a id="s-5235ab52dc"></a>`type`: `"object"`
- <a id="s-47172ec03c"></a>`additionalProperties`: `false`
- <a id="s-126ced84d8"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-69b72d206d"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8035295f62"></a>`collection_id` | yes | [CollectionId](#s-7f283b7e62) |  |
| <a id="s-5cf79b0bc0"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5c44a90899"></a>definition `EvaluationBinding`

- <a id="s-87fd55a8d3"></a>`type`: `"object"`
- <a id="s-17928cffe8"></a>`additionalProperties`: `false`
- <a id="s-24125e8c30"></a>`required`: `["evaluation_id","matrix_sha256","variant_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e6cec6ac90"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0210bb5624"></a>`matrix_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9d45337df0"></a>`parameters` | no | type="object"; additionalProperties=([JsonValue](#s-6239d9390d)) |  |
| <a id="s-ed30156f6e"></a>`variant_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-8da12a9a78"></a>definition `JoinWorkBinding`

- <a id="s-acfa357f36"></a>`type`: `"object"`
- <a id="s-abe0298251"></a>`additionalProperties`: `false`
- <a id="s-ddcf6c910a"></a>`required`: `["parent_work_id","branch_set_sha256","members"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-da683125ff"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1b32059271"></a>`kind` | no | type="string"; const="join"; default="join" |  |
| <a id="s-5b12d6119a"></a>`members` | yes | type="array"; items=([JoinWorkMemberBinding](#s-bb9e7aba49)); minItems=2 |  |
| <a id="s-b11564521b"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-bb9e7aba49"></a>definition `JoinWorkMemberBinding`

- <a id="s-f88131a8e6"></a>`type`: `"object"`
- <a id="s-060f5da890"></a>`additionalProperties`: `false`
- <a id="s-5c3c19dad6"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-80696a35fe"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-40e965ae79"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-30d67cbf41"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-ca07fb858a"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-6239d9390d"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-0868d9822c"></a>definition `RecipeIdentityRef`

- <a id="s-943287084b"></a>`type`: `"object"`
- <a id="s-ad58cfaa8d"></a>`additionalProperties`: `false`
- <a id="s-398e6efa43"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bc1fa7f0ae"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-84ff88415f"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-da07ad6116"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_inputs](stove0-protocol-workidentity-canonical-inputs.md)
- [root_identities](stove0-protocol-workidentity-root-identities.md)
- [seal](stove0-protocol-workidentity-seal.md)
- [verify_digest](stove0-protocol-workidentity-verify-digest.md)

## Governing policies

- <a id="pa-bd606aa428"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d0ea7752956422585c8d2c6b132762c4330ffc78510d2ec0c13ce705e24fd0c4 -->

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
        "RecipeIdentityRef": {
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
    },
    "signature": "\"(*, format: Literal['stove0-work/v1'] = 'stove0-work/v1', recipe: stove0_protocol.models.RecipeIdentityRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootIdentityRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, evaluation: stove0_protocol.models.EvaluationBinding | None = None, fork_join: Optional[Annotated[stove0_protocol.models.BranchWorkBinding | stove0_protocol.models.JoinWorkBinding, FieldInfo(annotation=NoneType, required=True, discriminator='kind')]] = None, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkIdentity",
  "unit": "export"
}
```

</details>
