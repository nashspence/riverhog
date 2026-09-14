# riverhog_protocol.ProcessingClaimDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimdocument:26bbd50563 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-75863d0da4"></a>
- <a id="s-3a9e8ca942"></a>`distribution`: `riverhog-protocol`
- <a id="s-0c651f9b71"></a>`module`: `riverhog_protocol`
- <a id="s-17efc6ff7d"></a>`name`: `ProcessingClaimDocument`
- <a id="s-1118c39165"></a>`unit`: `export`

### Declared structure

- <a id="s-fca0c2e8a3"></a>`kind`: `"class"`
- <a id="s-9ec17805fb"></a>`signature`: `"\"(*, format: Literal['riverhog-processing-claim/v1'], id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], consumer: riverhog_protocol.collection_workflow_transport.ProcessingClaimConsumerDocument, purpose: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], state: Literal['active', 'settled', 'retiring', 'abandoned', 'released'], fence: Annotated[int, Ge(ge=1)], expires_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], created_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], settled_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, abandoned_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, abandonment_reason: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=1000)] = None, released_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, output_collection_id: CollectionId \| None = None, work_document: dict[str, typing.Any], work_document_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: riverhog_protocol.collection_workflow_transport.ReceivingSetDocument, plan: riverhog_protocol.collection_workflow_transport.ProcessingClaimPlanDocument \| None = None, outcomes: riverhog_protocol.collection_workflow_transport.OutcomeSetDocument, outcome_settlement: riverhog_protocol.collection_workflow_transport.ProcessingClaimOutcomeSettlementDocument \| None = None) -> None\""`

#### Validated model schema

<a id="s-75d5c675f7"></a>
- <a id="s-ea69d118e0"></a>`title`: ProcessingClaimDocument
- <a id="s-adb9d9f057"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed124be02c"></a>`abandoned_at` | no | anyOf=type="string"; minLength=1; maxLength=64 \| type="null" |  |
| <a id="s-361ff7cea9"></a>`abandonment_reason` | no | anyOf=type="string"; minLength=1; maxLength=1000 \| type="null" |  |
| <a id="s-53711020c5"></a>`consumer` | yes | #/$defs/ProcessingClaimConsumerDocument |  |
| <a id="s-fedd7a0acb"></a>`created_at` | yes | type="string"; minLength=1; maxLength=64 |  |
| <a id="s-273f7eec10"></a>`expires_at` | yes | type="string"; minLength=1; maxLength=64 |  |
| <a id="s-cfc44f9e08"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-bebb4904f4"></a>`format` | yes | type="string"; const="riverhog-processing-claim/v1" |  |
| <a id="s-90373974ca"></a>`id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a50c2084a7"></a>`inputs` | yes | #/$defs/ReceivingSetDocument |  |
| <a id="s-dd6da4834d"></a>`outcome_settlement` | no | anyOf=#/$defs/ProcessingClaimOutcomeSettlementDocument \| type="null" |  |
| <a id="s-6d065e28b7"></a>`outcomes` | yes | #/$defs/OutcomeSetDocument |  |
| <a id="s-4fd9d45af3"></a>`output_collection_id` | no | anyOf=#/$defs/CollectionId \| type="null" |  |
| <a id="s-a4ffc938e5"></a>`plan` | no | anyOf=#/$defs/ProcessingClaimPlanDocument \| type="null" |  |
| <a id="s-c744090071"></a>`purpose` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-d1e10a80ab"></a>`released_at` | no | anyOf=type="string"; minLength=1; maxLength=64 \| type="null" |  |
| <a id="s-17bea566dd"></a>`settled_at` | no | anyOf=type="string"; minLength=1; maxLength=64 \| type="null" |  |
| <a id="s-ef952ef113"></a>`state` | yes | type="string"; enum=["active","settled","retiring","abandoned","released"] |  |
| <a id="s-34b72d57f0"></a>`updated_at` | yes | type="string"; minLength=1; maxLength=64 |  |
| <a id="s-7b3a390533"></a>`work_document` | yes | type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` |  |
| <a id="s-e58e8a592c"></a>`work_document_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9d8834edff"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-7c0e8d7455"></a>`ArtifactSetAuthorityDocument` | type="object"; fields=`count`, `sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-896ee01c47"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-52c5516811"></a>`ExactSetAuthorityDocument` | type="object"; fields=`count`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-e96e2dd600"></a>`OperationIdentityDocument` | type="object"; fields=`id`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-f7d5d1ec59"></a>`OutcomeSetDocument` | type="object"; fields=`authority`, `count`, `failure`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-d6d970f005"></a>`ProcessingClaimConsumerDocument` | type="object"; fields=`app`, `key_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-e2510804b4"></a>`ProcessingClaimOutcomeSettlementDocument` | type="object"; fields=`outcomes`, `retirement_grace_seconds`, `retirement_policy`; additional keys=`additionalProperties`, `if`, `required`, `then` |
| <a id="s-c3d365e968"></a>`ProcessingClaimPlanDocument` | type="object"; fields=`artifacts`, `controller_evidence`, `controller_evidence_sha256`, `execution_id`, `inputs`, `operation`, `retirement_grace_seconds`, `retirement_policy`, `sealed_at`; additional keys=`additionalProperties`, `if`, `required`, `then` |
| <a id="s-9dd4ce27cb"></a>`ReceivingSetDocument` | type="object"; fields=`authority`, `count`, `state`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.ProcessingClaimDocument.validate_claim](riverhog-protocol-processingclaimdocument-validate-claim.md)

## Governing policies

- <a id="pa-f88ab499ee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2dafd08bbb236f131be3156a6a0a10356f03b1673ff45c9e0b9c427f2838be4d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactSetAuthorityDocument": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "title": "Count",
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "title": "Total Bytes",
              "type": "integer"
            }
          },
          "required": [
            "count",
            "sha256",
            "total_bytes"
          ],
          "title": "ArtifactSetAuthorityDocument",
          "type": "object"
        },
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "ExactSetAuthorityDocument": {
          "additionalProperties": false,
          "description": "Small immutable identity for an exact canonically ordered logical set.",
          "properties": {
            "count": {
              "minimum": 1,
              "title": "Count",
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            }
          },
          "required": [
            "count",
            "sha256"
          ],
          "title": "ExactSetAuthorityDocument",
          "type": "object"
        },
        "OperationIdentityDocument": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Id",
              "type": "string"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256"
          ],
          "title": "OperationIdentityDocument",
          "type": "object"
        },
        "OutcomeSetDocument": {
          "additionalProperties": false,
          "properties": {
            "authority": {
              "anyOf": [
                {
                  "$ref": "#/$defs/ExactSetAuthorityDocument"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "count": {
              "minimum": 0,
              "title": "Count",
              "type": "integer"
            },
            "failure": {
              "anyOf": [
                {
                  "maxLength": 1000,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Failure"
            },
            "state": {
              "enum": [
                "receiving",
                "sealing",
                "sealed",
                "failed"
              ],
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "state",
            "count"
          ],
          "title": "OutcomeSetDocument",
          "type": "object"
        },
        "ProcessingClaimConsumerDocument": {
          "additionalProperties": false,
          "properties": {
            "app": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "App",
              "type": "string"
            },
            "key_id": {
              "anyOf": [
                {
                  "maxLength": 300,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Key Id"
            }
          },
          "required": [
            "app"
          ],
          "title": "ProcessingClaimConsumerDocument",
          "type": "object"
        },
        "ProcessingClaimOutcomeSettlementDocument": {
          "additionalProperties": false,
          "if": {
            "properties": {
              "retirement_policy": {
                "const": "retain"
              }
            }
          },
          "properties": {
            "outcomes": {
              "$ref": "#/$defs/ExactSetAuthorityDocument"
            },
            "retirement_grace_seconds": {
              "minimum": 0,
              "title": "Retirement Grace Seconds",
              "type": "integer"
            },
            "retirement_policy": {
              "enum": [
                "retain",
                "retire-after-verified-output"
              ],
              "title": "Retirement Policy",
              "type": "string"
            }
          },
          "required": [
            "outcomes",
            "retirement_policy",
            "retirement_grace_seconds"
          ],
          "then": {
            "properties": {
              "retirement_grace_seconds": {
                "const": 0
              }
            }
          },
          "title": "ProcessingClaimOutcomeSettlementDocument",
          "type": "object"
        },
        "ProcessingClaimPlanDocument": {
          "additionalProperties": false,
          "if": {
            "properties": {
              "retirement_policy": {
                "const": "retain"
              }
            }
          },
          "properties": {
            "artifacts": {
              "$ref": "#/$defs/ArtifactSetAuthorityDocument"
            },
            "controller_evidence": {
              "additionalProperties": true,
              "title": "Controller Evidence",
              "type": "object",
              "x-riverhog-encoded-bytes-max": 16777216,
              "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-controller-evidence-envelope"
              }
            },
            "controller_evidence_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Controller Evidence Sha256",
              "type": "string"
            },
            "execution_id": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Execution Id",
              "type": "string"
            },
            "inputs": {
              "$ref": "#/$defs/ExactSetAuthorityDocument"
            },
            "operation": {
              "$ref": "#/$defs/OperationIdentityDocument"
            },
            "retirement_grace_seconds": {
              "minimum": 0,
              "title": "Retirement Grace Seconds",
              "type": "integer"
            },
            "retirement_policy": {
              "enum": [
                "retain",
                "retire-after-verified-output"
              ],
              "title": "Retirement Policy",
              "type": "string"
            },
            "sealed_at": {
              "maxLength": 64,
              "minLength": 1,
              "title": "Sealed At",
              "type": "string"
            }
          },
          "required": [
            "execution_id",
            "controller_evidence",
            "controller_evidence_sha256",
            "operation",
            "inputs",
            "artifacts",
            "retirement_policy",
            "retirement_grace_seconds",
            "sealed_at"
          ],
          "then": {
            "properties": {
              "retirement_grace_seconds": {
                "const": 0
              }
            }
          },
          "title": "ProcessingClaimPlanDocument",
          "type": "object"
        },
        "ReceivingSetDocument": {
          "additionalProperties": false,
          "properties": {
            "authority": {
              "anyOf": [
                {
                  "$ref": "#/$defs/ExactSetAuthorityDocument"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "count": {
              "minimum": 0,
              "title": "Count",
              "type": "integer"
            },
            "state": {
              "enum": [
                "receiving",
                "sealed"
              ],
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "state",
            "count"
          ],
          "title": "ReceivingSetDocument",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "allOf": [
        {
          "else": {
            "properties": {
              "settled_at": {
                "type": "null"
              }
            }
          },
          "if": {
            "properties": {
              "state": {
                "enum": [
                  "settled",
                  "retiring",
                  "released"
                ]
              }
            }
          },
          "then": {
            "properties": {
              "settled_at": {
                "type": "string"
              }
            },
            "required": [
              "settled_at"
            ]
          }
        },
        {
          "else": {
            "properties": {
              "abandoned_at": {
                "type": "null"
              },
              "abandonment_reason": {
                "type": "null"
              }
            }
          },
          "if": {
            "properties": {
              "state": {
                "const": "abandoned"
              }
            }
          },
          "then": {
            "properties": {
              "abandoned_at": {
                "type": "string"
              },
              "abandonment_reason": {
                "type": "string"
              }
            },
            "required": [
              "abandoned_at",
              "abandonment_reason"
            ]
          }
        },
        {
          "else": {
            "properties": {
              "released_at": {
                "type": "null"
              }
            }
          },
          "if": {
            "properties": {
              "state": {
                "const": "released"
              }
            }
          },
          "then": {
            "properties": {
              "released_at": {
                "type": "string"
              }
            },
            "required": [
              "released_at"
            ]
          }
        }
      ],
      "properties": {
        "abandoned_at": {
          "anyOf": [
            {
              "maxLength": 64,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Abandoned At"
        },
        "abandonment_reason": {
          "anyOf": [
            {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Abandonment Reason"
        },
        "consumer": {
          "$ref": "#/$defs/ProcessingClaimConsumerDocument"
        },
        "created_at": {
          "maxLength": 64,
          "minLength": 1,
          "title": "Created At",
          "type": "string"
        },
        "expires_at": {
          "maxLength": 64,
          "minLength": 1,
          "title": "Expires At",
          "type": "string"
        },
        "fence": {
          "minimum": 1,
          "title": "Fence",
          "type": "integer"
        },
        "format": {
          "const": "riverhog-processing-claim/v1",
          "title": "Format",
          "type": "string"
        },
        "id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Id",
          "type": "string"
        },
        "inputs": {
          "$ref": "#/$defs/ReceivingSetDocument"
        },
        "outcome_settlement": {
          "anyOf": [
            {
              "$ref": "#/$defs/ProcessingClaimOutcomeSettlementDocument"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "outcomes": {
          "$ref": "#/$defs/OutcomeSetDocument"
        },
        "output_collection_id": {
          "anyOf": [
            {
              "$ref": "#/$defs/CollectionId"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "plan": {
          "anyOf": [
            {
              "$ref": "#/$defs/ProcessingClaimPlanDocument"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "purpose": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Purpose",
          "type": "string"
        },
        "released_at": {
          "anyOf": [
            {
              "maxLength": 64,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Released At"
        },
        "settled_at": {
          "anyOf": [
            {
              "maxLength": 64,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Settled At"
        },
        "state": {
          "enum": [
            "active",
            "settled",
            "retiring",
            "abandoned",
            "released"
          ],
          "title": "State",
          "type": "string"
        },
        "updated_at": {
          "maxLength": 64,
          "minLength": 1,
          "title": "Updated At",
          "type": "string"
        },
        "work_document": {
          "additionalProperties": true,
          "title": "Work Document",
          "type": "object",
          "x-riverhog-encoded-bytes-max": 4194304,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-work-document-envelope"
          }
        },
        "work_document_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Work Document Sha256",
          "type": "string"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Work Id",
          "type": "string"
        }
      },
      "required": [
        "format",
        "id",
        "work_id",
        "consumer",
        "purpose",
        "state",
        "fence",
        "expires_at",
        "created_at",
        "updated_at",
        "work_document",
        "work_document_sha256",
        "inputs",
        "outcomes"
      ],
      "title": "ProcessingClaimDocument",
      "type": "object"
    },
    "signature": "\"(*, format: Literal['riverhog-processing-claim/v1'], id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], consumer: riverhog_protocol.collection_workflow_transport.ProcessingClaimConsumerDocument, purpose: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], state: Literal['active', 'settled', 'retiring', 'abandoned', 'released'], fence: Annotated[int, Ge(ge=1)], expires_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], created_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], settled_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, abandoned_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, abandonment_reason: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=1000)] = None, released_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, output_collection_id: CollectionId | None = None, work_document: dict[str, typing.Any], work_document_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: riverhog_protocol.collection_workflow_transport.ReceivingSetDocument, plan: riverhog_protocol.collection_workflow_transport.ProcessingClaimPlanDocument | None = None, outcomes: riverhog_protocol.collection_workflow_transport.OutcomeSetDocument, outcome_settlement: riverhog_protocol.collection_workflow_transport.ProcessingClaimOutcomeSettlementDocument | None = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimDocument",
  "unit": "export"
}
```
