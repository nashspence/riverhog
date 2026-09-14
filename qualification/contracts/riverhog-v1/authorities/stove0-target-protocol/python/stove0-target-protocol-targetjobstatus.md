# stove0_target_protocol.TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetjobstatus:dc067334d4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31cb463d3b"></a>
- <a id="s-48531cee14"></a>`distribution`: `stove0-target-protocol`
- <a id="s-0ba1fd98bc"></a>`module`: `stove0_target_protocol`
- <a id="s-ed20c46453"></a>`name`: `TargetJobStatus`
- <a id="s-4aa842cf1e"></a>`unit`: `export`

### Declared structure

- <a id="s-3e23c88925"></a>`kind`: `"class"`
- <a id="s-e0720959f8"></a>`signature`: `"\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['queued', 'running', 'canceling', 'interrupted', 'inapplicable', 'succeeded', 'failed', 'canceled'], attempt: Annotated[int, Ge(ge=1)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], progress: stove0_target_protocol.protocol.TargetProgress, production: stove0_target_protocol.protocol.TargetProductionAuthority \| None = None, output_collection: stove0_target_protocol.protocol.OutputCollectionRef \| None = None, execution_evidence: stove0_target_protocol.protocol.TargetExecutionEvidence \| None = None, derivation: dict[str, typing.Any] \| None = None, effect_receipt: stove0_target_protocol.protocol.ExternalEffectReceipt \| None = None, failure: stove0_target_protocol.protocol.TargetFailure \| None = None, inapplicable: stove0_target_protocol.protocol.TargetInapplicable \| None = None) -> None\""`

#### Validated model schema

<a id="s-66d919c56b"></a>
- <a id="s-7716c43357"></a>`title`: TargetJobStatus
- <a id="s-e05cbbd276"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e863ddeeab"></a>`attempt` | yes | type="integer"; minimum=1 |  |
| <a id="s-34af17894f"></a>`derivation` | no | anyOf=type="object"; additional keys=`additionalProperties` \| type="null" |  |
| <a id="s-0004a645e9"></a>`effect_receipt` | no | anyOf=#/$defs/ExternalEffectReceipt \| type="null" |  |
| <a id="s-e170293c3a"></a>`execution_evidence` | no | anyOf=#/$defs/TargetExecutionEvidence \| type="null" |  |
| <a id="s-c2358d4323"></a>`failure` | no | anyOf=#/$defs/TargetFailure \| type="null" |  |
| <a id="s-16b9c1ba29"></a>`inapplicable` | no | anyOf=#/$defs/TargetInapplicable \| type="null" |  |
| <a id="s-0fef5e9472"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fdfddb84b6"></a>`output_collection` | no | anyOf=#/$defs/OutputCollectionRef \| type="null" |  |
| <a id="s-b330728783"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bb2a827a04"></a>`production` | no | anyOf=#/$defs/TargetProductionAuthority \| type="null" |  |
| <a id="s-768559147f"></a>`progress` | yes | #/$defs/TargetProgress |  |
| <a id="s-2cdfa080a8"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"] |  |
| <a id="s-83e05f830d"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-78a639312f"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-eec126ab97"></a>`ArtifactDispositionSetIdentity` | type="object"; fields=`disposition_count`, `output_artifact_count`, `output_edge_count`, `sha256`; additional keys=`required` |
| <a id="s-532ff66dff"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-fa9e50683e"></a>`ExternalEffectReceipt` | type="object"; fields=`execution_sha256`, `format`, `job_id`, `operation_contract_sha256`, `plan_sha256`, `receipt_sha256`, `request_sha256`, `result`, `target_contract_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-2b1e5f66ee"></a>`JsonValue` | empty object |
| <a id="s-1c00d8dab5"></a>`OutputArtifactRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-d29bfb8a33"></a>`OutputArtifactSetIdentity` | type="object"; fields=`artifact_count`, `roles`, `sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-0fc4bfc6da"></a>`OutputCollectionRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `derivation_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-f1b5facb9e"></a>`TargetExecutionEvidence` | type="object"; fields=`execution_sha256`, `operation_contract_sha256`, `plan_sha256`, `runtime`, `target_contract_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-985a78c617"></a>`TargetFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-efacb42ecf"></a>`TargetInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-faf2b63b18"></a>`TargetProductionAuthority` | type="object"; fields=`disposition_count`, `disposition_sha256`, `format`, `job_id`, `outputs`, `plan_sha256`, `production_sha256`, `riverhog_disposition_set`, `source_edge_count`, `source_edge_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-ddb01229e7"></a>`TargetProgress` | type="object"; fields=`completed`, `phase`, `total`, `unit`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetJobStatus.canonical_derivation](stove0-target-protocol-targetjobstatus-canonical-derivation.md)
- [stove0_target_protocol.TargetJobStatus.validate_terminal_shape](stove0-target-protocol-targetjobstatus-validate-terminal-shape.md)

## Governing policies

- <a id="pa-aa7780be93"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetJobStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 96da57dddcae17d37a739b941c40246025c66344e618c62912f598b3d3f18fcc -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactDispositionSetIdentity": {
          "description": "Small identity for one sealed claim-scoped relational disposition set.",
          "properties": {
            "disposition_count": {
              "title": "Disposition Count",
              "type": "integer"
            },
            "output_artifact_count": {
              "title": "Output Artifact Count",
              "type": "integer"
            },
            "output_edge_count": {
              "title": "Output Edge Count",
              "type": "integer"
            },
            "sha256": {
              "title": "Sha256",
              "type": "string"
            }
          },
          "required": [
            "disposition_count",
            "output_edge_count",
            "output_artifact_count",
            "sha256"
          ],
          "title": "ArtifactDispositionSetIdentity",
          "type": "object"
        },
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "ExternalEffectReceipt": {
          "additionalProperties": false,
          "properties": {
            "execution_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Execution Sha256",
              "type": "string"
            },
            "format": {
              "const": "stove0-external-effect-receipt/v1",
              "default": "stove0-external-effect-receipt/v1",
              "title": "Format",
              "type": "string"
            },
            "job_id": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Job Id",
              "type": "string"
            },
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Operation Contract Sha256",
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Plan Sha256",
              "type": "string"
            },
            "receipt_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Receipt Sha256",
              "type": "string"
            },
            "request_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Request Sha256",
              "type": "string"
            },
            "result": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "title": "Result",
              "type": "object",
              "x-riverhog-encoded-bytes-max": 65536,
              "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-external-effect-receipt"
              }
            },
            "target_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Target Contract Sha256",
              "type": "string"
            }
          },
          "required": [
            "job_id",
            "request_sha256",
            "target_contract_sha256",
            "operation_contract_sha256",
            "plan_sha256",
            "execution_sha256",
            "result",
            "receipt_sha256"
          ],
          "title": "ExternalEffectReceipt",
          "type": "object"
        },
        "JsonValue": {},
        "OutputArtifactRoleCount": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "title": "Count",
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Role",
              "type": "string"
            }
          },
          "required": [
            "role",
            "count"
          ],
          "title": "OutputArtifactRoleCount",
          "type": "object"
        },
        "OutputArtifactSetIdentity": {
          "additionalProperties": false,
          "description": "Small identity for target outputs already registered with Riverhog.",
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "title": "Artifact Count",
              "type": "integer"
            },
            "roles": {
              "items": {
                "$ref": "#/$defs/OutputArtifactRoleCount"
              },
              "minItems": 1,
              "title": "Roles",
              "type": "array"
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
            "artifact_count",
            "total_bytes",
            "roles",
            "sha256"
          ],
          "title": "OutputArtifactSetIdentity",
          "type": "object"
        },
        "OutputCollectionRef": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Archive Root Sha256",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Content Identity",
              "type": "string"
            },
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Derivation Sha256",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity",
            "derivation_sha256"
          ],
          "title": "OutputCollectionRef",
          "type": "object"
        },
        "TargetExecutionEvidence": {
          "additionalProperties": false,
          "properties": {
            "execution_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Execution Sha256",
              "type": "string"
            },
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Operation Contract Sha256",
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Plan Sha256",
              "type": "string"
            },
            "runtime": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "title": "Runtime",
              "type": "object"
            },
            "target_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Target Contract Sha256",
              "type": "string"
            }
          },
          "required": [
            "target_contract_sha256",
            "operation_contract_sha256",
            "plan_sha256",
            "execution_sha256"
          ],
          "title": "TargetExecutionEvidence",
          "type": "object"
        },
        "TargetFailure": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Code",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "title": "Message",
              "type": "string"
            },
            "retryable": {
              "title": "Retryable",
              "type": "boolean"
            }
          },
          "required": [
            "code",
            "message",
            "retryable"
          ],
          "title": "TargetFailure",
          "type": "object"
        },
        "TargetInapplicable": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Code",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "title": "Message",
              "type": "string"
            }
          },
          "required": [
            "code",
            "message"
          ],
          "title": "TargetInapplicable",
          "type": "object"
        },
        "TargetProductionAuthority": {
          "additionalProperties": false,
          "properties": {
            "disposition_count": {
              "minimum": 1,
              "title": "Disposition Count",
              "type": "integer"
            },
            "disposition_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Disposition Sha256",
              "type": "string"
            },
            "format": {
              "const": "stove0-target-production/v1",
              "default": "stove0-target-production/v1",
              "title": "Format",
              "type": "string"
            },
            "job_id": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Job Id",
              "type": "string"
            },
            "outputs": {
              "$ref": "#/$defs/OutputArtifactSetIdentity"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Plan Sha256",
              "type": "string"
            },
            "production_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Production Sha256",
              "type": "string"
            },
            "riverhog_disposition_set": {
              "$ref": "#/$defs/ArtifactDispositionSetIdentity"
            },
            "source_edge_count": {
              "minimum": 1,
              "title": "Source Edge Count",
              "type": "integer"
            },
            "source_edge_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Source Edge Sha256",
              "type": "string"
            }
          },
          "required": [
            "job_id",
            "plan_sha256",
            "outputs",
            "disposition_count",
            "disposition_sha256",
            "source_edge_count",
            "source_edge_sha256",
            "riverhog_disposition_set",
            "production_sha256"
          ],
          "title": "TargetProductionAuthority",
          "type": "object"
        },
        "TargetProgress": {
          "additionalProperties": false,
          "properties": {
            "completed": {
              "minimum": 0,
              "title": "Completed",
              "type": "integer"
            },
            "phase": {
              "maxLength": 120,
              "minLength": 1,
              "title": "Phase",
              "type": "string"
            },
            "total": {
              "anyOf": [
                {
                  "minimum": 0,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Total"
            },
            "unit": {
              "anyOf": [
                {
                  "maxLength": 40,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Unit"
            }
          },
          "required": [
            "phase",
            "completed"
          ],
          "title": "TargetProgress",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "allOf": [
        {
          "else": {
            "properties": {
              "failure": {
                "type": "null"
              }
            }
          },
          "if": {
            "properties": {
              "state": {
                "const": "failed"
              }
            }
          },
          "then": {
            "properties": {
              "failure": {
                "type": "object"
              }
            },
            "required": [
              "failure"
            ]
          }
        }
      ],
      "properties": {
        "attempt": {
          "minimum": 1,
          "title": "Attempt",
          "type": "integer"
        },
        "derivation": {
          "anyOf": [
            {
              "additionalProperties": true,
              "type": "object"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Derivation"
        },
        "effect_receipt": {
          "anyOf": [
            {
              "$ref": "#/$defs/ExternalEffectReceipt"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "execution_evidence": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetExecutionEvidence"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "failure": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetFailure"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "inapplicable": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetInapplicable"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Job Id",
          "type": "string"
        },
        "output_collection": {
          "anyOf": [
            {
              "$ref": "#/$defs/OutputCollectionRef"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "production": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetProductionAuthority"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "progress": {
          "$ref": "#/$defs/TargetProgress"
        },
        "protocol": {
          "default": "stove0-transform-target/v1",
          "enum": [
            "stove0-transform-target/v1",
            "stove0-effect-target/v1"
          ],
          "title": "Protocol",
          "type": "string"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Sha256",
          "type": "string"
        },
        "state": {
          "enum": [
            "queued",
            "running",
            "canceling",
            "interrupted",
            "inapplicable",
            "succeeded",
            "failed",
            "canceled"
          ],
          "title": "State",
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "state",
        "attempt",
        "request_sha256",
        "plan_sha256",
        "progress"
      ],
      "title": "TargetJobStatus",
      "type": "object"
    },
    "signature": "\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['queued', 'running', 'canceling', 'interrupted', 'inapplicable', 'succeeded', 'failed', 'canceled'], attempt: Annotated[int, Ge(ge=1)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], progress: stove0_target_protocol.protocol.TargetProgress, production: stove0_target_protocol.protocol.TargetProductionAuthority | None = None, output_collection: stove0_target_protocol.protocol.OutputCollectionRef | None = None, execution_evidence: stove0_target_protocol.protocol.TargetExecutionEvidence | None = None, derivation: dict[str, typing.Any] | None = None, effect_receipt: stove0_target_protocol.protocol.ExternalEffectReceipt | None = None, failure: stove0_target_protocol.protocol.TargetFailure | None = None, inapplicable: stove0_target_protocol.protocol.TargetInapplicable | None = None) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetJobStatus",
  "unit": "export"
}
```
