# stove0-target-jobs: AcceptedTargetJob

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-target-jobs:stove0-target-jobs-acceptedtargetjob:9f4d08e69f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-jobs](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-895464670c"></a>
- Document: `AcceptedTargetJob`

### Document schema

<a id="s-e1f1741ba6"></a>
- <a id="s-6e33c0235a"></a>`title`: AcceptedTargetJob
- <a id="s-73f0ef927b"></a>`description`: Durable, non-secret identity of one accepted target job request.
- <a id="s-be8c95f916"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-405daa5385"></a>`declaration` | yes | #/$defs/TargetJobDeclaration |  |
| <a id="s-61c2c95c95"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-973e2a3365"></a>`ArtifactSelectionRef` | type="object"; fields=`artifact_count`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-6be2fdc787"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-548c2def56"></a>`BranchWorkBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `decision_sha256`, `kind`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-cc22d9cbdd"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-3b4ac5edde"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-5e0010cbf3"></a>`ControllerEvidence` | type="object"; fields=`controller_evidence_sha256`, `execution_envelope`, `format`; additional keys=`additionalProperties`, `required` |
| <a id="s-9469da79f1"></a>`EffectPlan` | type="object"; fields=`inputs`, `intent`, `observation_result_sha256s`, `operation_contract_sha256`, `operation_id`, `plan_sha256`, `protocol`, `target_contract_sha256`, `target_implementation_id`, `target_options`; additional keys=`additionalProperties`, `required` |
| <a id="s-f025905d6a"></a>`EvaluationBinding` | type="object"; fields=`evaluation_id`, `matrix_sha256`, `parameters`, `variant_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-033c077b2b"></a>`ExecutionEnvelope` | type="object"; fields=`claim_id`, `execution_envelope_sha256`, `fence`, `format`, `target_plan`, `workflow_plan`; additional keys=`additionalProperties`, `required` |
| <a id="s-53a18f36fb"></a>`JoinWorkBinding` | type="object"; fields=`branch_set_sha256`, `kind`, `members`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-82dd57ddfa"></a>`JoinWorkMemberBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `producer_settlement_sha256`, `settlement_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-8c4f8b0d2b"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-326d0b99e9"></a>`JsonValue` | empty object |
| <a id="s-35224d38b2"></a>`ObservationEvidence` | type="object"; fields=`request`, `result`; additional keys=`additionalProperties`, `required` |
| <a id="s-ee43532728"></a>`ObservationFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-063d130a3c"></a>`ObservationInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-df4feebae2"></a>`ObservationRequest` | type="object"; fields=`format`, `maximum_result_bytes`, `observer_contract_id`, `observer_contract_sha256`, `observer_descriptor_sha256`, `observer_registration_id`, `options`, `request_id`, `retrieval_policy`, `subjects`, `timeout_seconds`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-3073671d72"></a>`ObservationResult` | type="object"; fields=`execution_evidence`, `facts`, `facts_schema`, `facts_sha256`, `failure`, `format`, `inapplicable`, `observer`, `observer_contract_id`, `observer_contract_sha256`, `request_id`, `result_sha256`, `state`, `subjects`; additional keys=`additionalProperties`, `required` |
| <a id="s-c48c7036bd"></a>`ObserverImplementation` | type="object"; fields=`descriptor_sha256`, `id`, `protocol`, `source_revision`, `version`; additional keys=`additionalProperties`, `required` |
| <a id="s-e89fe7446a"></a>`OperationRef` | type="object"; fields=`id`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-cee329be31"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-31aadefae6"></a>`TargetInputAuthority` | type="object"; fields=`roles`, `selection`; additional keys=`additionalProperties`, `required` |
| <a id="s-491d231ef9"></a>`TargetInputRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-91f55fb248"></a>`TargetJobDeclaration` | type="object"; fields=`claim_id`, `controller_evidence`, `fence`, `job_id`, `plan`, `workspace_assurance`; additional keys=`additionalProperties`, `required` |
| <a id="s-9e23b75bbf"></a>`TargetPlanBinding` | type="object"; fields=`operation_contract_sha256`, `plan`, `plan_sha256`, `protocol`, `target_contract_sha256`, `target_implementation_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-a54e765c9b"></a>`TransformPlan` | type="object"; fields=`inputs`, `intent`, `observation_result_sha256s`, `operation_contract_sha256`, `operation_id`, `plan_sha256`, `protocol`, `target_contract_sha256`, `target_implementation_id`, `target_options`; additional keys=`additionalProperties`, `required` |
| <a id="s-7b040f3044"></a>`WorkIdentity` | type="object"; fields=`effective_intent`, `evaluation`, `fork_join`, `format`, `inputs`, `recipe`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-7278f1b514"></a>`WorkflowPlan` | type="object"; fields=`format`, `input_retrieval_policy`, `observations`, `operation`, `output_policy`, `requested_target_options`, `result_kind`, `retirement_grace_seconds`, `retirement_policy`, `target_contract_sha256`, `target_registration_id`, `work`, `workflow_plan_sha256`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0-target-jobs durable-state identity](stove0-target-jobs-durable-state-identity.md)

## Governing policies

- <a id="pa-899b241fe4"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:stove0-target-jobs](../../../evidence/sources.md#src-7b4138829a) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/protocol.py`

### Machine authority

- `/external_contract/durable_state/owners/5/structure/documents/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bfd68b20b927724bb6a948754e7626732e555bd7c38d79a2f1524e14a408f0e4 -->

```json
{
  "id": "AcceptedTargetJob",
  "schema": {
    "$defs": {
      "ArtifactSelectionRef": {
        "additionalProperties": false,
        "description": "Closed reference to a separately retained selection document.",
        "properties": {
          "artifact_count": {
            "minimum": 1,
            "title": "Artifact Count",
            "type": "integer"
          },
          "selection_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Selection Sha256",
            "type": "string"
          },
          "total_bytes": {
            "minimum": 0,
            "title": "Total Bytes",
            "type": "integer"
          }
        },
        "required": [
          "selection_sha256",
          "artifact_count",
          "total_bytes"
        ],
        "title": "ArtifactSelectionRef",
        "type": "object"
      },
      "ArtifactSubject": {
        "additionalProperties": false,
        "properties": {
          "bytes": {
            "minimum": 0,
            "title": "Bytes",
            "type": "integer"
          },
          "collection": {
            "$ref": "#/$defs/CollectionRootRef"
          },
          "id": {
            "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
            "title": "Id",
            "type": "string"
          },
          "media_type": {
            "anyOf": [
              {
                "maxLength": 255,
                "minLength": 1,
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "default": null,
            "title": "Media Type"
          },
          "path": {
            "maxLength": 4096,
            "minLength": 1,
            "title": "Path",
            "type": "string"
          },
          "role": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Role",
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
          "role",
          "collection",
          "path",
          "bytes",
          "sha256"
        ],
        "title": "ArtifactSubject",
        "type": "object"
      },
      "BranchWorkBinding": {
        "additionalProperties": false,
        "description": "Stable parent/branch lineage for one ordinary child work identity.",
        "properties": {
          "artifact_selection_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Artifact Selection Sha256",
            "type": "string"
          },
          "branch_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Branch Id",
            "type": "string"
          },
          "decision_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Decision Sha256",
            "type": "string"
          },
          "kind": {
            "const": "branch",
            "default": "branch",
            "title": "Kind",
            "type": "string"
          },
          "parent_work_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Parent Work Id",
            "type": "string"
          }
        },
        "required": [
          "parent_work_id",
          "branch_id",
          "decision_sha256",
          "artifact_selection_sha256"
        ],
        "title": "BranchWorkBinding",
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
          }
        },
        "required": [
          "collection_id",
          "archive_root_sha256",
          "content_identity"
        ],
        "title": "CollectionRootRef",
        "type": "object"
      },
      "ControllerEvidence": {
        "additionalProperties": false,
        "properties": {
          "controller_evidence_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Controller Evidence Sha256",
            "type": "string"
          },
          "execution_envelope": {
            "$ref": "#/$defs/ExecutionEnvelope"
          },
          "format": {
            "const": "stove0-controller-evidence/v1",
            "default": "stove0-controller-evidence/v1",
            "title": "Format",
            "type": "string"
          }
        },
        "required": [
          "execution_envelope",
          "controller_evidence_sha256"
        ],
        "title": "ControllerEvidence",
        "type": "object"
      },
      "EffectPlan": {
        "additionalProperties": false,
        "properties": {
          "inputs": {
            "$ref": "#/$defs/TargetInputAuthority"
          },
          "intent": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Intent",
            "type": "object"
          },
          "observation_result_sha256s": {
            "default": [],
            "items": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "title": "Observation Result Sha256S",
            "type": "array"
          },
          "operation_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Operation Contract Sha256",
            "type": "string"
          },
          "operation_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Operation Id",
            "type": "string"
          },
          "plan_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Plan Sha256",
            "type": "string"
          },
          "protocol": {
            "const": "stove0-effect-target/v1",
            "default": "stove0-effect-target/v1",
            "title": "Protocol",
            "type": "string"
          },
          "target_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Contract Sha256",
            "type": "string"
          },
          "target_implementation_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Target Implementation Id",
            "type": "string"
          },
          "target_options": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Target Options",
            "type": "object"
          }
        },
        "required": [
          "operation_id",
          "operation_contract_sha256",
          "inputs",
          "intent",
          "target_implementation_id",
          "target_contract_sha256",
          "plan_sha256"
        ],
        "title": "EffectPlan",
        "type": "object"
      },
      "EvaluationBinding": {
        "additionalProperties": false,
        "description": "Immutable membership of one work item in a trial/evaluation matrix.",
        "properties": {
          "evaluation_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Evaluation Id",
            "type": "string"
          },
          "matrix_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Matrix Sha256",
            "type": "string"
          },
          "parameters": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Parameters",
            "type": "object"
          },
          "variant_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Variant Id",
            "type": "string"
          }
        },
        "required": [
          "evaluation_id",
          "matrix_sha256",
          "variant_id"
        ],
        "title": "EvaluationBinding",
        "type": "object"
      },
      "ExecutionEnvelope": {
        "additionalProperties": false,
        "properties": {
          "claim_id": {
            "maxLength": 160,
            "minLength": 1,
            "title": "Claim Id",
            "type": "string"
          },
          "execution_envelope_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Execution Envelope Sha256",
            "type": "string"
          },
          "fence": {
            "minimum": 1,
            "title": "Fence",
            "type": "integer"
          },
          "format": {
            "const": "stove0-execution-envelope/v1",
            "default": "stove0-execution-envelope/v1",
            "title": "Format",
            "type": "string"
          },
          "target_plan": {
            "$ref": "#/$defs/TargetPlanBinding"
          },
          "workflow_plan": {
            "$ref": "#/$defs/WorkflowPlan"
          }
        },
        "required": [
          "claim_id",
          "fence",
          "workflow_plan",
          "target_plan",
          "execution_envelope_sha256"
        ],
        "title": "ExecutionEnvelope",
        "type": "object"
      },
      "JoinWorkBinding": {
        "additionalProperties": false,
        "description": "Stable branch-set lineage for one ordinary join work identity.",
        "properties": {
          "branch_set_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Branch Set Sha256",
            "type": "string"
          },
          "kind": {
            "const": "join",
            "default": "join",
            "title": "Kind",
            "type": "string"
          },
          "members": {
            "items": {
              "$ref": "#/$defs/JoinWorkMemberBinding"
            },
            "minItems": 2,
            "title": "Members",
            "type": "array"
          },
          "parent_work_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Parent Work Id",
            "type": "string"
          }
        },
        "required": [
          "parent_work_id",
          "branch_set_sha256",
          "members"
        ],
        "title": "JoinWorkBinding",
        "type": "object"
      },
      "JoinWorkMemberBinding": {
        "additionalProperties": false,
        "description": "Exact successful branch result used to derive one join work identity.",
        "properties": {
          "artifact_selection_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Artifact Selection Sha256",
            "type": "string"
          },
          "branch_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Branch Id",
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
            "default": null,
            "title": "Producer Settlement Sha256"
          },
          "settlement_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Settlement Sha256",
            "type": "string"
          }
        },
        "required": [
          "branch_id",
          "settlement_sha256",
          "artifact_selection_sha256"
        ],
        "title": "JoinWorkMemberBinding",
        "type": "object"
      },
      "JsonSchemaDocument": {
        "additionalProperties": false,
        "properties": {
          "dialect": {
            "const": "https://json-schema.org/draft/2020-12/schema",
            "default": "https://json-schema.org/draft/2020-12/schema",
            "title": "Dialect",
            "type": "string"
          },
          "format_policy": {
            "const": "annotation-only",
            "default": "annotation-only",
            "title": "Format Policy",
            "type": "string"
          },
          "id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Id",
            "type": "string"
          },
          "schema": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Schema",
            "type": "object"
          },
          "sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Sha256",
            "type": "string"
          }
        },
        "required": [
          "id",
          "sha256",
          "schema"
        ],
        "title": "JsonSchemaDocument",
        "type": "object"
      },
      "JsonValue": {},
      "ObservationEvidence": {
        "additionalProperties": false,
        "description": "Complete routing evidence: immutable request plus accepted result.",
        "properties": {
          "request": {
            "$ref": "#/$defs/ObservationRequest"
          },
          "result": {
            "$ref": "#/$defs/ObservationResult"
          }
        },
        "required": [
          "request",
          "result"
        ],
        "title": "ObservationEvidence",
        "type": "object"
      },
      "ObservationFailure": {
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
        "title": "ObservationFailure",
        "type": "object"
      },
      "ObservationInapplicable": {
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
        "title": "ObservationInapplicable",
        "type": "object"
      },
      "ObservationRequest": {
        "additionalProperties": false,
        "properties": {
          "format": {
            "const": "stove0-observation-request/v1",
            "default": "stove0-observation-request/v1",
            "title": "Format",
            "type": "string"
          },
          "maximum_result_bytes": {
            "default": 1048576,
            "maximum": 67108864,
            "minimum": 1,
            "title": "Maximum Result Bytes",
            "type": "integer"
          },
          "observer_contract_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Observer Contract Id",
            "type": "string"
          },
          "observer_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Observer Contract Sha256",
            "type": "string"
          },
          "observer_descriptor_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Observer Descriptor Sha256",
            "type": "string"
          },
          "observer_registration_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
            "title": "Observer Registration Id",
            "type": "string"
          },
          "options": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Options",
            "type": "object"
          },
          "request_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Request Id",
            "type": "string"
          },
          "retrieval_policy": {
            "default": "available-only",
            "enum": [
              "available-only",
              "allow"
            ],
            "title": "Retrieval Policy",
            "type": "string"
          },
          "subjects": {
            "items": {
              "$ref": "#/$defs/ArtifactSubject"
            },
            "minItems": 1,
            "title": "Subjects",
            "type": "array"
          },
          "timeout_seconds": {
            "default": 300,
            "maximum": 86400,
            "minimum": 1,
            "title": "Timeout Seconds",
            "type": "integer"
          },
          "work_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Work Id",
            "type": "string"
          }
        },
        "required": [
          "work_id",
          "observer_registration_id",
          "observer_descriptor_sha256",
          "observer_contract_id",
          "observer_contract_sha256",
          "subjects",
          "request_id"
        ],
        "title": "ObservationRequest",
        "type": "object"
      },
      "ObservationResult": {
        "additionalProperties": false,
        "properties": {
          "execution_evidence": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Execution Evidence",
            "type": "object"
          },
          "facts": {
            "anyOf": [
              {
                "additionalProperties": {
                  "$ref": "#/$defs/JsonValue"
                },
                "type": "object"
              },
              {
                "type": "null"
              }
            ],
            "default": null,
            "title": "Facts"
          },
          "facts_schema": {
            "anyOf": [
              {
                "$ref": "#/$defs/JsonSchemaDocument"
              },
              {
                "type": "null"
              }
            ],
            "default": null
          },
          "facts_sha256": {
            "anyOf": [
              {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "default": null,
            "title": "Facts Sha256"
          },
          "failure": {
            "anyOf": [
              {
                "$ref": "#/$defs/ObservationFailure"
              },
              {
                "type": "null"
              }
            ],
            "default": null
          },
          "format": {
            "const": "stove0-observation-result/v1",
            "default": "stove0-observation-result/v1",
            "title": "Format",
            "type": "string"
          },
          "inapplicable": {
            "anyOf": [
              {
                "$ref": "#/$defs/ObservationInapplicable"
              },
              {
                "type": "null"
              }
            ],
            "default": null
          },
          "observer": {
            "$ref": "#/$defs/ObserverImplementation"
          },
          "observer_contract_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Observer Contract Id",
            "type": "string"
          },
          "observer_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Observer Contract Sha256",
            "type": "string"
          },
          "request_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Request Id",
            "type": "string"
          },
          "result_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Result Sha256",
            "type": "string"
          },
          "state": {
            "enum": [
              "observed",
              "inapplicable",
              "failed",
              "canceled"
            ],
            "title": "State",
            "type": "string"
          },
          "subjects": {
            "items": {
              "$ref": "#/$defs/ArtifactSubject"
            },
            "minItems": 1,
            "title": "Subjects",
            "type": "array"
          }
        },
        "required": [
          "request_id",
          "state",
          "observer",
          "observer_contract_id",
          "observer_contract_sha256",
          "subjects",
          "result_sha256"
        ],
        "title": "ObservationResult",
        "type": "object"
      },
      "ObserverImplementation": {
        "additionalProperties": false,
        "properties": {
          "descriptor_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Descriptor Sha256",
            "type": "string"
          },
          "id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Id",
            "type": "string"
          },
          "protocol": {
            "const": "stove0-content-observer/v1",
            "default": "stove0-content-observer/v1",
            "title": "Protocol",
            "type": "string"
          },
          "source_revision": {
            "maxLength": 200,
            "minLength": 1,
            "title": "Source Revision",
            "type": "string"
          },
          "version": {
            "maxLength": 120,
            "minLength": 1,
            "title": "Version",
            "type": "string"
          }
        },
        "required": [
          "id",
          "version",
          "source_revision",
          "descriptor_sha256"
        ],
        "title": "ObserverImplementation",
        "type": "object"
      },
      "OperationRef": {
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
        "title": "OperationRef",
        "type": "object"
      },
      "RecipeRef": {
        "additionalProperties": false,
        "properties": {
          "id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Id",
            "type": "string"
          },
          "revision": {
            "minimum": 1,
            "title": "Revision",
            "type": "integer"
          },
          "sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Sha256",
            "type": "string"
          }
        },
        "required": [
          "id",
          "revision",
          "sha256"
        ],
        "title": "RecipeRef",
        "type": "object"
      },
      "TargetInputAuthority": {
        "additionalProperties": false,
        "description": "Small exact input authority retained by Stove0 and traversed in bounded pages.",
        "properties": {
          "roles": {
            "items": {
              "$ref": "#/$defs/TargetInputRoleCount"
            },
            "minItems": 1,
            "title": "Roles",
            "type": "array"
          },
          "selection": {
            "$ref": "#/$defs/ArtifactSelectionRef"
          }
        },
        "required": [
          "selection",
          "roles"
        ],
        "title": "TargetInputAuthority",
        "type": "object"
      },
      "TargetInputRoleCount": {
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
        "title": "TargetInputRoleCount",
        "type": "object"
      },
      "TargetJobDeclaration": {
        "additionalProperties": false,
        "properties": {
          "claim_id": {
            "maxLength": 160,
            "minLength": 1,
            "title": "Claim Id",
            "type": "string"
          },
          "controller_evidence": {
            "$ref": "#/$defs/ControllerEvidence"
          },
          "fence": {
            "minimum": 1,
            "title": "Fence",
            "type": "integer"
          },
          "job_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Job Id",
            "type": "string"
          },
          "plan": {
            "discriminator": {
              "mapping": {
                "stove0-effect-target/v1": "#/$defs/EffectPlan",
                "stove0-transform-target/v1": "#/$defs/TransformPlan"
              },
              "propertyName": "protocol"
            },
            "oneOf": [
              {
                "$ref": "#/$defs/TransformPlan"
              },
              {
                "$ref": "#/$defs/EffectPlan"
              }
            ],
            "title": "Plan"
          },
          "workspace_assurance": {
            "enum": [
              "encrypted",
              "ephemeral"
            ],
            "title": "Workspace Assurance",
            "type": "string"
          }
        },
        "required": [
          "job_id",
          "claim_id",
          "fence",
          "controller_evidence",
          "plan",
          "workspace_assurance"
        ],
        "title": "TargetJobDeclaration",
        "type": "object"
      },
      "TargetPlanBinding": {
        "additionalProperties": false,
        "description": "Opaque binding to a target-owned preflight plan.\n\nThe target protocol owns the plan schema and canonicalization algorithm. stove0\nretains the complete validated plan document and its target-issued digest, but\ndeliberately does not reinterpret or re-hash the plan with stove0's canonical\nJSON rules. This prevents two authorities from disagreeing about target plan\nidentity while preserving the full document in the execution envelope.",
        "properties": {
          "operation_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Operation Contract Sha256",
            "type": "string"
          },
          "plan": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Plan",
            "type": "object"
          },
          "plan_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Plan Sha256",
            "type": "string"
          },
          "protocol": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Protocol",
            "type": "string"
          },
          "target_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Contract Sha256",
            "type": "string"
          },
          "target_implementation_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Target Implementation Id",
            "type": "string"
          }
        },
        "required": [
          "protocol",
          "target_implementation_id",
          "target_contract_sha256",
          "operation_contract_sha256",
          "plan",
          "plan_sha256"
        ],
        "title": "TargetPlanBinding",
        "type": "object"
      },
      "TransformPlan": {
        "additionalProperties": false,
        "properties": {
          "inputs": {
            "$ref": "#/$defs/TargetInputAuthority"
          },
          "intent": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Intent",
            "type": "object"
          },
          "observation_result_sha256s": {
            "default": [],
            "items": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "title": "Observation Result Sha256S",
            "type": "array"
          },
          "operation_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Operation Contract Sha256",
            "type": "string"
          },
          "operation_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Operation Id",
            "type": "string"
          },
          "plan_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Plan Sha256",
            "type": "string"
          },
          "protocol": {
            "const": "stove0-transform-target/v1",
            "default": "stove0-transform-target/v1",
            "title": "Protocol",
            "type": "string"
          },
          "target_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Contract Sha256",
            "type": "string"
          },
          "target_implementation_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Target Implementation Id",
            "type": "string"
          },
          "target_options": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Target Options",
            "type": "object"
          }
        },
        "required": [
          "operation_id",
          "operation_contract_sha256",
          "inputs",
          "intent",
          "target_implementation_id",
          "target_contract_sha256",
          "plan_sha256"
        ],
        "title": "TransformPlan",
        "type": "object"
      },
      "WorkIdentity": {
        "additionalProperties": false,
        "properties": {
          "effective_intent": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Effective Intent",
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
            "default": null,
            "title": "Fork Join"
          },
          "format": {
            "const": "stove0-work/v1",
            "default": "stove0-work/v1",
            "title": "Format",
            "type": "string"
          },
          "inputs": {
            "items": {
              "$ref": "#/$defs/CollectionRootRef"
            },
            "minItems": 1,
            "title": "Inputs",
            "type": "array"
          },
          "recipe": {
            "$ref": "#/$defs/RecipeRef"
          },
          "work_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Work Id",
            "type": "string"
          }
        },
        "required": [
          "recipe",
          "inputs",
          "work_id"
        ],
        "title": "WorkIdentity",
        "type": "object"
      },
      "WorkflowPlan": {
        "additionalProperties": false,
        "properties": {
          "format": {
            "const": "stove0-workflow-plan/v1",
            "default": "stove0-workflow-plan/v1",
            "title": "Format",
            "type": "string"
          },
          "input_retrieval_policy": {
            "default": "available-only",
            "enum": [
              "available-only",
              "allow"
            ],
            "title": "Input Retrieval Policy",
            "type": "string"
          },
          "observations": {
            "default": [],
            "items": {
              "$ref": "#/$defs/ObservationEvidence"
            },
            "title": "Observations",
            "type": "array"
          },
          "operation": {
            "$ref": "#/$defs/OperationRef"
          },
          "output_policy": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Output Policy",
            "type": "object"
          },
          "requested_target_options": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Requested Target Options",
            "type": "object"
          },
          "result_kind": {
            "default": "collection",
            "enum": [
              "collection",
              "external-effect"
            ],
            "title": "Result Kind",
            "type": "string"
          },
          "retirement_grace_seconds": {
            "default": 0,
            "minimum": 0,
            "title": "Retirement Grace Seconds",
            "type": "integer"
          },
          "retirement_policy": {
            "default": "retain",
            "enum": [
              "retain",
              "retire-after-verified-output"
            ],
            "title": "Retirement Policy",
            "type": "string"
          },
          "target_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Contract Sha256",
            "type": "string"
          },
          "target_registration_id": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
            "title": "Target Registration Id",
            "type": "string"
          },
          "work": {
            "$ref": "#/$defs/WorkIdentity"
          },
          "workflow_plan_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Workflow Plan Sha256",
            "type": "string"
          }
        },
        "required": [
          "work",
          "operation",
          "target_registration_id",
          "target_contract_sha256",
          "workflow_plan_sha256"
        ],
        "title": "WorkflowPlan",
        "type": "object"
      }
    },
    "additionalProperties": false,
    "description": "Durable, non-secret identity of one accepted target job request.",
    "properties": {
      "declaration": {
        "$ref": "#/$defs/TargetJobDeclaration"
      },
      "request_sha256": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Request Sha256",
        "type": "string"
      }
    },
    "required": [
      "declaration",
      "request_sha256"
    ],
    "title": "AcceptedTargetJob",
    "type": "object"
  }
}
```
