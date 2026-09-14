# stove0_protocol.JoinDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joindeclaration:3bfa77fe24 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-97c387fe33"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-58eb35b67f"></a>`effective_intent` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-2e664fb177"></a>`format` | no | type="string"; const="stove0-join-declaration/v1" |  |
| <a id="s-efd68c962f"></a>`join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ae30d7711c"></a>`members` | yes | type="array"; minItems=2; items=(#/$defs/JoinMemberDeclaration) |  |
| <a id="s-904fa8d437"></a>`recipe` | yes | #/$defs/RecipeRef |  |
| <a id="s-5850f02fef"></a>`workflow_intent` | yes | #/$defs/WorkflowPlanIntent |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-5300c97a15"></a>`JoinMemberDeclaration` | type="object"; fields=`branch_id`, `output_roles`; additional keys=`additionalProperties`, `required` |
| <a id="s-3c52221150"></a>`JsonValue` | empty object |
| <a id="s-75a94902c0"></a>`OperationRef` | type="object"; fields=`id`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-659c9b0200"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-8705882e44"></a>`WorkflowPlanIntent` | type="object"; fields=`input_retrieval_policy`, `operation`, `output_policy`, `requested_target_options`, `result_kind`, `retirement_grace_seconds`, `retirement_policy`, `target_contract_sha256`, `target_registration_id`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_protocol.JoinDeclaration.canonical_members](stove0-protocol-joindeclaration-canonical-members.md)
- [stove0_protocol.JoinDeclaration.seal](stove0-protocol-joindeclaration-seal.md)
- [stove0_protocol.JoinDeclaration.verify_contract](stove0-protocol-joindeclaration-verify-contract.md)

## Governing policies

- <a id="pa-3288ab02f2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinDeclaration`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a14c938ac61b53cbbedc670936912fa7e28068f934bd1578a7faef982a33b597 -->

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
            "target_contract_sha256": {
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
            "target_contract_sha256"
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
