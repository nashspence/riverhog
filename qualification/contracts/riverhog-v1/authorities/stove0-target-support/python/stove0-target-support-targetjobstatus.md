# stove0_target_support.TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetjobstatus:435d58f64e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31dee2c892"></a>
- <a id="s-e87c0d32c3"></a>`distribution`: `stove0-target-support`
- <a id="s-33192005b7"></a>`module`: `stove0_target_support`
- <a id="s-f849fc5078"></a>`name`: `TargetJobStatus`
- <a id="s-f2c263a80f"></a>`unit`: `export`

### Declared structure

- <a id="s-c1869de810"></a>`kind`: `"class"`
- <a id="s-b6093c4b29"></a>`signature`: `"\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['queued', 'running', 'canceling', 'interrupted', 'inapplicable', 'succeeded', 'failed', 'canceled'], attempt: Annotated[int, Ge(ge=1)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], progress: stove0_target_protocol.protocol.TargetProgress, production: stove0_target_protocol.protocol.TargetProductionAuthority \| None = None, output_collection: stove0_target_protocol.protocol.OutputCollectionRef \| None = None, execution_evidence: stove0_target_protocol.protocol.TargetExecutionEvidence \| None = None, derivation: dict[str, typing.Any] \| None = None, effect_receipt: stove0_target_protocol.protocol.ExternalEffectReceipt \| None = None, failure: stove0_target_protocol.protocol.TargetFailure \| None = None, inapplicable: stove0_target_protocol.protocol.TargetInapplicable \| None = None) -> None\""`

#### Validated model schema

<a id="s-089320ef43"></a>
- <a id="s-b49df94a65"></a>`title`: TargetJobStatus
- <a id="s-07503b41ad"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-be1a7e3f6a"></a>`attempt` | yes | type="integer"; minimum=1 |  |
| <a id="s-9207d0abd0"></a>`derivation` | no | anyOf=type="object"; additional keys=`additionalProperties` \| type="null" |  |
| <a id="s-5cb268587a"></a>`effect_receipt` | no | anyOf=#/$defs/ExternalEffectReceipt \| type="null" |  |
| <a id="s-09c3346538"></a>`execution_evidence` | no | anyOf=#/$defs/TargetExecutionEvidence \| type="null" |  |
| <a id="s-40371aa134"></a>`failure` | no | anyOf=#/$defs/TargetFailure \| type="null" |  |
| <a id="s-ec449597d8"></a>`inapplicable` | no | anyOf=#/$defs/TargetInapplicable \| type="null" |  |
| <a id="s-d15136e338"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ec4ab910f8"></a>`output_collection` | no | anyOf=#/$defs/OutputCollectionRef \| type="null" |  |
| <a id="s-5ee0ffcdcc"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-27c910e141"></a>`production` | no | anyOf=#/$defs/TargetProductionAuthority \| type="null" |  |
| <a id="s-7180e43356"></a>`progress` | yes | #/$defs/TargetProgress |  |
| <a id="s-e0a3c41d92"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"] |  |
| <a id="s-bc529ccad9"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-eb75a0e443"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-5d080fa9ff"></a>`ArtifactDispositionSetIdentity` | type="object"; fields=`disposition_count`, `output_artifact_count`, `output_edge_count`, `sha256`; additional keys=`required` |
| <a id="s-2d02310cc8"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-8cf5a2952c"></a>`ExternalEffectReceipt` | type="object"; fields=`execution_sha256`, `format`, `job_id`, `operation_contract_sha256`, `plan_sha256`, `receipt_sha256`, `request_sha256`, `result`, `target_contract_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-517d880031"></a>`JsonValue` | empty object |
| <a id="s-66ea2baffe"></a>`OutputArtifactRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-710f5ba929"></a>`OutputArtifactSetIdentity` | type="object"; fields=`artifact_count`, `roles`, `sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-d4bd7f63aa"></a>`OutputCollectionRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `derivation_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-0db55b0df7"></a>`TargetExecutionEvidence` | type="object"; fields=`execution_sha256`, `operation_contract_sha256`, `plan_sha256`, `runtime`, `target_contract_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-230d3835b4"></a>`TargetFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-13a6f507d8"></a>`TargetInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-22b4e41004"></a>`TargetProductionAuthority` | type="object"; fields=`disposition_count`, `disposition_sha256`, `format`, `job_id`, `outputs`, `plan_sha256`, `production_sha256`, `riverhog_disposition_set`, `source_edge_count`, `source_edge_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-bcecdf072b"></a>`TargetProgress` | type="object"; fields=`completed`, `phase`, `total`, `unit`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetJobStatus.canonical_derivation](stove0-target-support-targetjobstatus-canonical-derivation.md)
- [stove0_target_support.TargetJobStatus.validate_terminal_shape](stove0-target-support-targetjobstatus-validate-terminal-shape.md)

## Governing policies

- <a id="pa-394eb7fb67"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetJobStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2e7a793781682e328d272c660bccd25c3802f4275ae1843789499c6596d9e1f -->

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
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetJobStatus",
  "unit": "export"
}
```
