# stove0_protocol.WorkIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workidentity:41554d3c31 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-f690998967"></a>`signature`: `"\"(*, format: Literal['stove0-work/v1'] = 'stove0-work/v1', recipe: stove0_protocol.models.RecipeRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, evaluation: stove0_protocol.models.EvaluationBinding \| None = None, fork_join: Optional[Annotated[stove0_protocol.models.BranchWorkBinding \| stove0_protocol.models.JoinWorkBinding, FieldInfo(annotation=NoneType, required=True, discriminator='kind')]] = None, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-72c2276867"></a>
- <a id="s-f458d0a678"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fd81682d93"></a>`effective_intent` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-c03e608b1b"></a>`evaluation` | no | anyOf=#/$defs/EvaluationBinding \| type="null" |  |
| <a id="s-d3499ce402"></a>`fork_join` | no | anyOf=oneOf=#/$defs/BranchWorkBinding \| #/$defs/JoinWorkBinding; additional keys=`discriminator` \| type="null" |  |
| <a id="s-023f2e2a82"></a>`format` | no | type="string"; const="stove0-work/v1" |  |
| <a id="s-e3be119012"></a>`inputs` | yes | type="array"; minItems=1; items=(#/$defs/CollectionRootRef) |  |
| <a id="s-bcb81b9438"></a>`recipe` | yes | #/$defs/RecipeRef |  |
| <a id="s-973dbaa04f"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-42e530e4b6"></a>`BranchWorkBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `decision_sha256`, `kind`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-7f283b7e62"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-6f33dceda8"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-5c44a90899"></a>`EvaluationBinding` | type="object"; fields=`evaluation_id`, `matrix_sha256`, `parameters`, `variant_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-8da12a9a78"></a>`JoinWorkBinding` | type="object"; fields=`branch_set_sha256`, `kind`, `members`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-bb9e7aba49"></a>`JoinWorkMemberBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `producer_settlement_sha256`, `settlement_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-6239d9390d"></a>`JsonValue` | empty object |
| <a id="s-367d1e8b47"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkIdentity.root_identities](stove0-protocol-workidentity-root-identities.md)
- [stove0_protocol.WorkIdentity.seal](stove0-protocol-workidentity-seal.md)
- [stove0_protocol.WorkIdentity.verify_digest](stove0-protocol-workidentity-verify-digest.md)

## Governing policies

- <a id="pa-bd606aa428"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkIdentity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 956af7eb61e9b97d3df585b0eef1d2c46e8103f39d5999cfc72fb3e76d27f2a8 -->

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
    "signature": "\"(*, format: Literal['stove0-work/v1'] = 'stove0-work/v1', recipe: stove0_protocol.models.RecipeRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, evaluation: stove0_protocol.models.EvaluationBinding | None = None, fork_join: Optional[Annotated[stove0_protocol.models.BranchWorkBinding | stove0_protocol.models.JoinWorkBinding, FieldInfo(annotation=NoneType, required=True, discriminator='kind')]] = None, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkIdentity",
  "unit": "export"
}
```
