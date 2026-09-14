# stove0_protocol.CoordinationSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationsettlement:899565cadc -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-df263685c6"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-931efbb7ae"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bc2661a6f6"></a>`children` | yes | type="array"; items=(#/$defs/CoordinationChildSettlementRef) |  |
| <a id="s-2baa0230b0"></a>`collection_result` | no | anyOf=#/$defs/CoordinationCollectionResult \| type="null" |  |
| <a id="s-f124166c20"></a>`contains_external_effects` | yes | type="boolean" |  |
| <a id="s-4c58376360"></a>`final_join_settlement_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-cf158a76e7"></a>`format` | no | type="string"; const="stove0-coordination-settlement/v1" |  |
| <a id="s-ef9f7ebb52"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-23c3d31743"></a>`work` | yes | #/$defs/WorkIdentity |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-92ca5a9d9d"></a>`ArtifactSelectionRef` | type="object"; fields=`artifact_count`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-c98157dc98"></a>`BranchWorkBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `decision_sha256`, `kind`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-6a15433853"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-dffa763174"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-15c3849639"></a>`CoordinationChildSettlementRef` | type="object"; fields=`branch_id`, `kind`, `settlement_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-74d5be0a2c"></a>`CoordinationCollectionResult` | type="object"; fields=`derivation_sha256`, `join_settlement_sha256`, `output_collection`, `output_selection`, `producer_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-fc51dad3c9"></a>`EvaluationBinding` | type="object"; fields=`evaluation_id`, `matrix_sha256`, `parameters`, `variant_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-b6c0046749"></a>`JoinWorkBinding` | type="object"; fields=`branch_set_sha256`, `kind`, `members`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-429d9c9e8b"></a>`JoinWorkMemberBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `producer_settlement_sha256`, `settlement_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-73f7ec6527"></a>`JsonValue` | empty object |
| <a id="s-cdf3d2df57"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-6d3d9fb2b1"></a>`WorkIdentity` | type="object"; fields=`effective_intent`, `evaluation`, `fork_join`, `format`, `inputs`, `recipe`, `work_id`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_protocol.CoordinationSettlement.canonical_children](stove0-protocol-coordinationsettlement-canonical-children.md)
- [stove0_protocol.CoordinationSettlement.seal](stove0-protocol-coordinationsettlement-seal.md)
- [stove0_protocol.CoordinationSettlement.verify_contract](stove0-protocol-coordinationsettlement-verify-contract.md)

## Governing policies

- <a id="pa-fa9286ce2f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationSettlement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 724afcbe96a5c937ab728d1dc2fceea828ec1b36d09e04518f0b7364292ada8c -->

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
              "$ref": "#/$defs/CollectionRootRef"
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
