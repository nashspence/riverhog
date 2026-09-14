# stove0_protocol.ExecutionEnvelope

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-executionenvelope:b14e4c5ac5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-98796758ff"></a>
- <a id="s-11312b4acc"></a>`distribution`: `stove0-protocol`
- <a id="s-065a90bc8c"></a>`module`: `stove0_protocol`
- <a id="s-1df444e898"></a>`name`: `ExecutionEnvelope`
- <a id="s-a99d2e2f02"></a>`unit`: `export`

### Declared structure

- <a id="s-bf717259f0"></a>`kind`: `"class"`
- <a id="s-6f89e7df38"></a>`signature`: `"\"(*, format: Literal['stove0-execution-envelope/v1'] = 'stove0-execution-envelope/v1', claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], workflow_plan: stove0_protocol.models.WorkflowPlan, target_plan: stove0_protocol.models.TargetPlanBinding, execution_envelope_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-94c7e6f47b"></a>
- <a id="s-b5eb575d75"></a>`title`: ExecutionEnvelope
- <a id="s-4aeccb5838"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4ac4602f5a"></a>`claim_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-efc57a14a8"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9d831e9a4e"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-ff0f62e1e0"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1" |  |
| <a id="s-0cf37e1416"></a>`target_plan` | yes | #/$defs/TargetPlanBinding |  |
| <a id="s-6e93adc7ef"></a>`workflow_plan` | yes | #/$defs/WorkflowPlan |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-c2b3b1b713"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-a4f6688f0e"></a>`BranchWorkBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `decision_sha256`, `kind`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-f85c49e8f3"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-fb99378fa5"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-2a7e5153b4"></a>`EvaluationBinding` | type="object"; fields=`evaluation_id`, `matrix_sha256`, `parameters`, `variant_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-97f6a66f1f"></a>`JoinWorkBinding` | type="object"; fields=`branch_set_sha256`, `kind`, `members`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-3bcbd9951b"></a>`JoinWorkMemberBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `producer_settlement_sha256`, `settlement_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-99ffea461d"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-1f3a5f4171"></a>`JsonValue` | empty object |
| <a id="s-887d61cf0e"></a>`ObservationEvidence` | type="object"; fields=`request`, `result`; additional keys=`additionalProperties`, `required` |
| <a id="s-770b8de811"></a>`ObservationFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-172c92b500"></a>`ObservationInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-8d6639d48f"></a>`ObservationRequest` | type="object"; fields=`format`, `maximum_result_bytes`, `observer_contract_id`, `observer_contract_sha256`, `observer_descriptor_sha256`, `observer_registration_id`, `options`, `request_id`, `retrieval_policy`, `subjects`, `timeout_seconds`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-0fcf7f4cb9"></a>`ObservationResult` | type="object"; fields=`execution_evidence`, `facts`, `facts_schema`, `facts_sha256`, `failure`, `format`, `inapplicable`, `observer`, `observer_contract_id`, `observer_contract_sha256`, `request_id`, `result_sha256`, `state`, `subjects`; additional keys=`additionalProperties`, `required` |
| <a id="s-fef42ac067"></a>`ObserverImplementation` | type="object"; fields=`descriptor_sha256`, `id`, `protocol`, `source_revision`, `version`; additional keys=`additionalProperties`, `required` |
| <a id="s-d292ac0013"></a>`OperationRef` | type="object"; fields=`id`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-6d448c123f"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-8b4bb871ab"></a>`TargetPlanBinding` | type="object"; fields=`operation_contract_sha256`, `plan`, `plan_sha256`, `protocol`, `target_contract_sha256`, `target_implementation_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-8bc614350a"></a>`WorkIdentity` | type="object"; fields=`effective_intent`, `evaluation`, `fork_join`, `format`, `inputs`, `recipe`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-10d3302684"></a>`WorkflowPlan` | type="object"; fields=`format`, `input_retrieval_policy`, `observations`, `operation`, `output_policy`, `requested_target_options`, `result_kind`, `retirement_grace_seconds`, `retirement_policy`, `target_contract_sha256`, `target_registration_id`, `work`, `workflow_plan_sha256`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_protocol.ExecutionEnvelope.seal](stove0-protocol-executionenvelope-seal.md)
- [stove0_protocol.ExecutionEnvelope.verify_digest](stove0-protocol-executionenvelope-verify-digest.md)

## Governing policies

- <a id="pa-b6b3381d3f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ExecutionEnvelope`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b3ecf4b84c97791368867268251cf3ef8b4c184e9ea8dbd0923fcded06d6eea2 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
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
    "signature": "\"(*, format: Literal['stove0-execution-envelope/v1'] = 'stove0-execution-envelope/v1', claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], workflow_plan: stove0_protocol.models.WorkflowPlan, target_plan: stove0_protocol.models.TargetPlanBinding, execution_envelope_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ExecutionEnvelope",
  "unit": "export"
}
```
