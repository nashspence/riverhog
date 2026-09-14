# riverhog_protocol.ProcessingClaimPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimpagedocument:0157b994c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-885d7fc9be"></a>
- <a id="s-2e64bcc8f5"></a>`distribution`: `riverhog-protocol`
- <a id="s-8ab1096825"></a>`module`: `riverhog_protocol`
- <a id="s-c6423f7606"></a>`name`: `ProcessingClaimPageDocument`
- <a id="s-22e70d1800"></a>`unit`: `export`

### Declared structure

- <a id="s-677df3beb5"></a>`kind`: `"class"`
- <a id="s-4959e67e4d"></a>`signature`: `"'(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken \| None, sort: ProcessingClaimSort, order: SortOrder, filters: riverhog_protocol.collection_workflow_transport.ProcessingClaimFiltersDocument, claims: list[riverhog_protocol.collection_workflow_transport.ProcessingClaimDocument]) -> None'"`

#### Validated model schema

<a id="s-25572c69a7"></a>
- <a id="s-9bb0cfcf47"></a>`title`: ProcessingClaimPageDocument
- <a id="s-6fd821e23a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9e01642313"></a>`claims` | yes | type="array"; items=(#/$defs/ProcessingClaimDocument) |  |
| <a id="s-431719c221"></a>`filters` | yes | #/$defs/ProcessingClaimFiltersDocument |  |
| <a id="s-3cde4d68f9"></a>`next_page_token` | yes | anyOf=#/$defs/BrowsePageToken \| type="null" |  |
| <a id="s-496eb27f70"></a>`order` | yes | #/$defs/SortOrder |  |
| <a id="s-6a13b81f20"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-8b86035dac"></a>`sort` | yes | #/$defs/ProcessingClaimSort |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-e2e3e3e591"></a>`ArtifactSetAuthorityDocument` | type="object"; fields=`count`, `sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-45f0983f59"></a>`BrowsePageToken` | type="string"; minLength=1; maxLength=8192 |
| <a id="s-40fd4aa997"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-b87ccb9da7"></a>`ExactSetAuthorityDocument` | type="object"; fields=`count`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-e05dec9278"></a>`OperationIdentityDocument` | type="object"; fields=`id`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-5b13a97ed1"></a>`OutcomeSetDocument` | type="object"; fields=`authority`, `count`, `failure`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-4bf8bbae51"></a>`ProcessingClaimConsumerDocument` | type="object"; fields=`app`, `key_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-0729737436"></a>`ProcessingClaimDocument` | type="object"; fields=`abandoned_at`, `abandonment_reason`, `consumer`, `created_at`, `expires_at`, `fence`, `format`, `id`, `inputs`, `outcome_settlement`, `outcomes`, `output_collection_id`, `plan`, `purpose`, `released_at`, `settled_at`, `state`, `updated_at`, `work_document`, `work_document_sha256`, `work_id`; allOf=additional keys=`else`, `if`, `then` \| additional keys=`else`, `if`, `then` \| additional keys=`else`, `if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-65f1deb5e5"></a>`ProcessingClaimFiltersDocument` | type="object"; fields=`state`; additional keys=`additionalProperties` |
| <a id="s-e159af631a"></a>`ProcessingClaimOutcomeSettlementDocument` | type="object"; fields=`outcomes`, `retirement_grace_seconds`, `retirement_policy`; additional keys=`additionalProperties`, `if`, `required`, `then` |
| <a id="s-3bbf0a4010"></a>`ProcessingClaimPlanDocument` | type="object"; fields=`artifacts`, `controller_evidence`, `controller_evidence_sha256`, `execution_id`, `inputs`, `operation`, `retirement_grace_seconds`, `retirement_policy`, `sealed_at`; additional keys=`additionalProperties`, `if`, `required`, `then` |
| <a id="s-3db543803a"></a>`ProcessingClaimSort` | type="string"; enum=["created_at","updated_at","expires_at","state","work_id","execution_id"] |
| <a id="s-4631ed20b1"></a>`ReceivingSetDocument` | type="object"; fields=`authority`, `count`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-8b46b0fee4"></a>`SortOrder` | type="string"; enum=["asc","desc"] |

## Governing policies

- <a id="pa-efe7905141"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimPageDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a7c6dd58f3a171a2a7b438cfeb72d5904c37a3330a1c5f8f488b00f9af3dca42 -->

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
        "BrowsePageToken": {
          "maxLength": 8192,
          "minLength": 1,
          "type": "string"
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
        "ProcessingClaimDocument": {
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
        "ProcessingClaimFiltersDocument": {
          "additionalProperties": false,
          "properties": {
            "state": {
              "anyOf": [
                {
                  "enum": [
                    "active",
                    "settled",
                    "retiring",
                    "abandoned",
                    "released"
                  ],
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "State"
            }
          },
          "title": "ProcessingClaimFiltersDocument",
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
        "ProcessingClaimSort": {
          "enum": [
            "created_at",
            "updated_at",
            "expires_at",
            "state",
            "work_id",
            "execution_id"
          ],
          "type": "string"
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
        },
        "SortOrder": {
          "enum": [
            "asc",
            "desc"
          ],
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "claims": {
          "items": {
            "$ref": "#/$defs/ProcessingClaimDocument"
          },
          "title": "Claims",
          "type": "array"
        },
        "filters": {
          "$ref": "#/$defs/ProcessingClaimFiltersDocument"
        },
        "next_page_token": {
          "anyOf": [
            {
              "$ref": "#/$defs/BrowsePageToken"
            },
            {
              "type": "null"
            }
          ]
        },
        "order": {
          "$ref": "#/$defs/SortOrder"
        },
        "page_size": {
          "maximum": 100,
          "minimum": 1,
          "title": "Page Size",
          "type": "integer"
        },
        "sort": {
          "$ref": "#/$defs/ProcessingClaimSort"
        }
      },
      "required": [
        "page_size",
        "next_page_token",
        "sort",
        "order",
        "filters",
        "claims"
      ],
      "title": "ProcessingClaimPageDocument",
      "type": "object"
    },
    "signature": "'(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, sort: ProcessingClaimSort, order: SortOrder, filters: riverhog_protocol.collection_workflow_transport.ProcessingClaimFiltersDocument, claims: list[riverhog_protocol.collection_workflow_transport.ProcessingClaimDocument]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimPageDocument",
  "unit": "export"
}
```
