# stove0_protocol.JoinDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joindeclaration:3bfa77fe24 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1b4ba953c4"></a>
- <a id="s-def1a1128f"></a>`distribution`: `stove0-protocol`
- <a id="s-61a837bb45"></a>`module`: `stove0_protocol`
- <a id="s-91afe9b9c8"></a>`name`: `JoinDeclaration`
- <a id="s-296c5c4a48"></a>`unit`: `export`

### Declared structure

- <a id="s-37b2fa2d28"></a>`kind`: `"class"`
- <a id="s-9ceaefdc39"></a>`signature`: `"\"(*, format: Literal['stove0-join-declaration/v1'] = 'stove0-join-declaration/v1', members: Annotated[tuple[stove0_protocol.fork_join.JoinMemberDeclaration, ...], MinLen(min_length=2)], recipe: stove0_protocol.models.RecipeRef, effective_intent: dict[str, JsonValue] = <factory>, workflow_intent: stove0_protocol.models.WorkflowPlanIntent, join_declaration_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-5fb9c36efa"></a>

- <a id="s-97c387fe33"></a>`type`: `"object"`
- <a id="s-9af88790fd"></a>`additionalProperties`: `false`
- <a id="s-a9e2cbc7cd"></a>`required`: `["members","recipe","workflow_intent","join_declaration_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-58eb35b67f"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-3c52221150)) |  |
| <a id="s-2e664fb177"></a>`format` | no | type="string"; const="stove0-join-declaration/v1"; default="stove0-join-declaration/v1" |  |
| <a id="s-efd68c962f"></a>`join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ae30d7711c"></a>`members` | yes | type="array"; items=([JoinMemberDeclaration](#s-5300c97a15)); minItems=2 |  |
| <a id="s-904fa8d437"></a>`recipe` | yes | [RecipeRef](#s-659c9b0200) |  |
| <a id="s-5850f02fef"></a>`workflow_intent` | yes | [WorkflowPlanIntent](#s-8705882e44) |  |

##### Definitions

- [JoinMemberDeclaration](#s-5300c97a15)
- [JsonValue](#s-3c52221150)
- [OperationRef](#s-75a94902c0)
- [RecipeRef](#s-659c9b0200)
- [WorkflowPlanIntent](#s-8705882e44)

##### <a id="s-5300c97a15"></a>definition `JoinMemberDeclaration`

- <a id="s-169eaadd8b"></a>`type`: `"object"`
- <a id="s-8edf453718"></a>`additionalProperties`: `false`
- <a id="s-79d345b1bf"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8a1bc774d9"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4eedb9a569"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-3c52221150"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-75a94902c0"></a>definition `OperationRef`

- <a id="s-0677662619"></a>`type`: `"object"`
- <a id="s-d3a3ef28de"></a>`additionalProperties`: `false`
- <a id="s-efed4252fd"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-70ba042203"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b0cc77cdee"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-659c9b0200"></a>definition `RecipeRef`

- <a id="s-7f528ede2c"></a>`type`: `"object"`
- <a id="s-8e541f4c41"></a>`additionalProperties`: `false`
- <a id="s-7788118628"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-075fba364e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-6e68457ccc"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-1e45b41794"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-8705882e44"></a>definition `WorkflowPlanIntent`

- <a id="s-e8e30226d9"></a>`type`: `"object"`
- <a id="s-716c9e864b"></a>`additionalProperties`: `false`
- <a id="s-9e9a5a8ded"></a>`required`: `["operation","target_registration_id","target_descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0a69e0e6f9"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-7a8d666867"></a>`operation` | yes | [OperationRef](#s-75a94902c0) |  |
| <a id="s-e4882b0236"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-3c52221150)) |  |
| <a id="s-de64dbd8be"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-3c52221150)) |  |
| <a id="s-664b25d4aa"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-9b1c8fb986"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-4b20943d92"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-16943e066f"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e46608b220"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [canonical_members](stove0-protocol-joindeclaration-canonical-members.md)
- [seal](stove0-protocol-joindeclaration-seal.md)
- [verify_contract](stove0-protocol-joindeclaration-verify-contract.md)

## Governing policies

- <a id="pa-3288ab02f2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JoinDeclaration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d0268c72d474ea5525e7cc4be17dd857e1bcc18a9a658c83a9a2db5c8115e317 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JoinMemberDeclaration": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "output_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "branch_id",
            "output_roles"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "OperationRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256"
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
        },
        "WorkflowPlanIntent": {
          "additionalProperties": false,
          "properties": {
            "input_retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "type": "string"
            },
            "operation": {
              "$ref": "#/$defs/OperationRef"
            },
            "output_policy": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "requested_target_options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "result_kind": {
              "default": "collection",
              "enum": [
                "collection",
                "external-effect"
              ],
              "type": "string"
            },
            "retirement_grace_seconds": {
              "default": 0,
              "minimum": 0,
              "type": "integer"
            },
            "retirement_policy": {
              "default": "retain",
              "enum": [
                "retain",
                "retire-after-verified-output"
              ],
              "type": "string"
            },
            "target_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "target_registration_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "operation",
            "target_registration_id",
            "target_descriptor_sha256"
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
        "format": {
          "const": "stove0-join-declaration/v1",
          "default": "stove0-join-declaration/v1",
          "type": "string"
        },
        "join_declaration_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "members": {
          "items": {
            "$ref": "#/$defs/JoinMemberDeclaration"
          },
          "minItems": 2,
          "type": "array"
        },
        "recipe": {
          "$ref": "#/$defs/RecipeRef"
        },
        "workflow_intent": {
          "$ref": "#/$defs/WorkflowPlanIntent"
        }
      },
      "required": [
        "members",
        "recipe",
        "workflow_intent",
        "join_declaration_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-join-declaration/v1'] = 'stove0-join-declaration/v1', members: Annotated[tuple[stove0_protocol.fork_join.JoinMemberDeclaration, ...], MinLen(min_length=2)], recipe: stove0_protocol.models.RecipeRef, effective_intent: dict[str, JsonValue] = <factory>, workflow_intent: stove0_protocol.models.WorkflowPlanIntent, join_declaration_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinDeclaration",
  "unit": "export"
}
```

</details>
