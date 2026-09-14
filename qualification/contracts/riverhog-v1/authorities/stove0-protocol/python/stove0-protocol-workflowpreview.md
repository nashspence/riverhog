# stove0_protocol.WorkflowPreview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreview:bbf475d7e1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-651702325b"></a>
- <a id="s-812cee1ddd"></a>`distribution`: `stove0-protocol`
- <a id="s-a0917f42b4"></a>`module`: `stove0_protocol`
- <a id="s-e79104d0ec"></a>`name`: `WorkflowPreview`
- <a id="s-d96e0e3c3f"></a>`unit`: `export`

### Declared structure

- <a id="s-3bd595da1e"></a>`kind`: `"class"`
- <a id="s-8250f12a24"></a>`signature`: `"\"(*, format: Literal['stove0-workflow-preview/v1'] = 'stove0-workflow-preview/v1', preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['ready', 'inapplicable', 'failed', 'canceled'], work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan \| None = None, branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = (), selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...] = (), target_plans: tuple[stove0_protocol.fork_join.BranchTargetPreview, ...] = (), outcome: stove0_protocol.models.PreviewOutcome \| None = None, warnings: tuple[str, ...] = (), preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-81a1ea6aa0"></a>
- <a id="s-ba49bd9c41"></a>`title`: WorkflowPreview
- <a id="s-8a04fe0c55"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0e42aa9676"></a>`branch_set_plan` | no | anyOf=#/$defs/BranchSetPlan \| type="null" |  |
| <a id="s-a76b72dca1"></a>`branch_sets` | no | type="array"; items=(#/$defs/BranchSetPlan) |  |
| <a id="s-3c88f9e333"></a>`format` | no | type="string"; const="stove0-workflow-preview/v1" |  |
| <a id="s-3d2b0ee24f"></a>`observations` | no | type="array"; items=(#/$defs/ObservationEvidence) |  |
| <a id="s-47ed167091"></a>`outcome` | no | anyOf=#/$defs/PreviewOutcome \| type="null" |  |
| <a id="s-87bb34d9ad"></a>`preview_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-451df9d341"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bdb06537dd"></a>`selections` | no | type="array"; items=(#/$defs/ArtifactSelection) |  |
| <a id="s-ed517e1872"></a>`state` | yes | type="string"; enum=["ready","inapplicable","failed","canceled"] |  |
| <a id="s-fc8e8c6b96"></a>`target_plans` | no | type="array"; items=(#/$defs/BranchTargetPreview) |  |
| <a id="s-d776a4e46e"></a>`warnings` | no | type="array"; items=(type="string") |  |
| <a id="s-878e671f1a"></a>`work` | yes | #/$defs/WorkIdentity |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-a714f2944c"></a>`ArtifactSelection` | type="object"; fields=`artifact_count`, `artifacts`, `format`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-c80df5a893"></a>`ArtifactSelectionRef` | type="object"; fields=`artifact_count`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-ac676d0abd"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-3f7e0f08b1"></a>`BranchPlan` | type="object"; fields=`artifact_selection`, `branch_id`, `kind`, `workflow_plan`; additional keys=`additionalProperties`, `required` |
| <a id="s-21ac64d411"></a>`BranchSetPlan` | type="object"; fields=`branch_set_sha256`, `branches`, `decision_sha256`, `evidence_sha256s`, `format`, `join`, `parent_work`, `retirement_grace_seconds`, `retirement_policy`; additional keys=`additionalProperties`, `required` |
| <a id="s-3c67d4e666"></a>`BranchTargetPreview` | type="object"; fields=`branch_id`, `target_plan`, `work_id`, `workflow_plan_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-b2eb508a56"></a>`BranchWorkBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `decision_sha256`, `kind`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-cb9942ab76"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-79d50387f6"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-cbec4e220b"></a>`CoordinationBranchPlan` | type="object"; fields=`artifact_selection`, `branch_id`, `branch_set_sha256`, `kind`, `work`; additional keys=`additionalProperties`, `required` |
| <a id="s-01ce021abb"></a>`EvaluationBinding` | type="object"; fields=`evaluation_id`, `matrix_sha256`, `parameters`, `variant_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-f8578c1e81"></a>`JoinDeclaration` | type="object"; fields=`effective_intent`, `format`, `join_declaration_sha256`, `members`, `recipe`, `workflow_intent`; additional keys=`additionalProperties`, `required` |
| <a id="s-6e947c9694"></a>`JoinMemberDeclaration` | type="object"; fields=`branch_id`, `output_roles`; additional keys=`additionalProperties`, `required` |
| <a id="s-d88819dfa6"></a>`JoinWorkBinding` | type="object"; fields=`branch_set_sha256`, `kind`, `members`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-018eaaf578"></a>`JoinWorkMemberBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `producer_settlement_sha256`, `settlement_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-d767c99029"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-8664a811cd"></a>`JsonValue` | empty object |
| <a id="s-57bdea281d"></a>`ObservationEvidence` | type="object"; fields=`request`, `result`; additional keys=`additionalProperties`, `required` |
| <a id="s-95c80dc12f"></a>`ObservationFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-f36dbf7834"></a>`ObservationInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-7591e81f68"></a>`ObservationRequest` | type="object"; fields=`format`, `maximum_result_bytes`, `observer_contract_id`, `observer_contract_sha256`, `observer_descriptor_sha256`, `observer_registration_id`, `options`, `request_id`, `retrieval_policy`, `subjects`, `timeout_seconds`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-589f253343"></a>`ObservationResult` | type="object"; fields=`execution_evidence`, `facts`, `facts_schema`, `facts_sha256`, `failure`, `format`, `inapplicable`, `observer`, `observer_contract_id`, `observer_contract_sha256`, `request_id`, `result_sha256`, `state`, `subjects`; additional keys=`additionalProperties`, `required` |
| <a id="s-7d26ff83dc"></a>`ObserverImplementation` | type="object"; fields=`descriptor_sha256`, `id`, `protocol`, `source_revision`, `version`; additional keys=`additionalProperties`, `required` |
| <a id="s-536e9e8cac"></a>`OperationRef` | type="object"; fields=`id`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-b22537e605"></a>`PreviewOutcome` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-d26d08db0c"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-58822f76be"></a>`TargetPlanBinding` | type="object"; fields=`operation_contract_sha256`, `plan`, `plan_sha256`, `protocol`, `target_contract_sha256`, `target_implementation_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-3869a9b08e"></a>`WorkIdentity` | type="object"; fields=`effective_intent`, `evaluation`, `fork_join`, `format`, `inputs`, `recipe`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-3bf71a6673"></a>`WorkflowPlan` | type="object"; fields=`format`, `input_retrieval_policy`, `observations`, `operation`, `output_policy`, `requested_target_options`, `result_kind`, `retirement_grace_seconds`, `retirement_policy`, `target_contract_sha256`, `target_registration_id`, `work`, `workflow_plan_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-8849589028"></a>`WorkflowPlanIntent` | type="object"; fields=`input_retrieval_policy`, `operation`, `output_policy`, `requested_target_options`, `result_kind`, `retirement_grace_seconds`, `retirement_policy`, `target_contract_sha256`, `target_registration_id`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkflowPreview.seal](stove0-protocol-workflowpreview-seal.md)
- [stove0_protocol.WorkflowPreview.verify_digest](stove0-protocol-workflowpreview-verify-digest.md)

## Governing policies

- <a id="pa-006a1cbc2a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreview`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 18dbf0ab7a692771ddcf504318c6efc1f2e0cf39bcecc02d12471d9f204169c0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactSelection": {
          "additionalProperties": false,
          "description": "One exact, content-addressed selection of immutable artifacts.",
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "title": "Artifact Count",
              "type": "integer"
            },
            "artifacts": {
              "items": {
                "$ref": "#/$defs/ArtifactSubject"
              },
              "minItems": 1,
              "title": "Artifacts",
              "type": "array"
            },
            "format": {
              "const": "stove0-artifact-selection/v1",
              "default": "stove0-artifact-selection/v1",
              "title": "Format",
              "type": "string"
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
            "artifacts",
            "artifact_count",
            "total_bytes",
            "selection_sha256"
          ],
          "title": "ArtifactSelection",
          "type": "object"
        },
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
        "BranchPlan": {
          "additionalProperties": false,
          "description": "One named required child work using the ordinary WorkflowPlan contract.",
          "properties": {
            "artifact_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Branch Id",
              "type": "string"
            },
            "kind": {
              "const": "leaf",
              "default": "leaf",
              "title": "Kind",
              "type": "string"
            },
            "workflow_plan": {
              "$ref": "#/$defs/WorkflowPlan"
            }
          },
          "required": [
            "branch_id",
            "artifact_selection",
            "workflow_plan"
          ],
          "title": "BranchPlan",
          "type": "object"
        },
        "BranchSetPlan": {
          "additionalProperties": false,
          "description": "One immutable set of required branches and one optional exact join.",
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Branch Set Sha256",
              "type": "string"
            },
            "branches": {
              "items": {
                "discriminator": {
                  "mapping": {
                    "coordination": "#/$defs/CoordinationBranchPlan",
                    "leaf": "#/$defs/BranchPlan"
                  },
                  "propertyName": "kind"
                },
                "oneOf": [
                  {
                    "$ref": "#/$defs/BranchPlan"
                  },
                  {
                    "$ref": "#/$defs/CoordinationBranchPlan"
                  }
                ]
              },
              "minItems": 1,
              "title": "Branches",
              "type": "array"
            },
            "decision_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Decision Sha256",
              "type": "string"
            },
            "evidence_sha256s": {
              "default": [],
              "items": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "title": "Evidence Sha256S",
              "type": "array"
            },
            "format": {
              "const": "stove0-branch-set/v1",
              "default": "stove0-branch-set/v1",
              "title": "Format",
              "type": "string"
            },
            "join": {
              "anyOf": [
                {
                  "$ref": "#/$defs/JoinDeclaration"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "parent_work": {
              "$ref": "#/$defs/WorkIdentity"
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
            }
          },
          "required": [
            "parent_work",
            "decision_sha256",
            "branches",
            "branch_set_sha256"
          ],
          "title": "BranchSetPlan",
          "type": "object"
        },
        "BranchTargetPreview": {
          "additionalProperties": false,
          "description": "Target-owned preflight evidence for one exact previewed branch.",
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Branch Id",
              "type": "string"
            },
            "target_plan": {
              "$ref": "#/$defs/TargetPlanBinding"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Work Id",
              "type": "string"
            },
            "workflow_plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Workflow Plan Sha256",
              "type": "string"
            }
          },
          "required": [
            "branch_id",
            "work_id",
            "workflow_plan_sha256",
            "target_plan"
          ],
          "title": "BranchTargetPreview",
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
        "CoordinationBranchPlan": {
          "additionalProperties": false,
          "description": "One named required branch-bound child coordinator.",
          "properties": {
            "artifact_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Branch Id",
              "type": "string"
            },
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Branch Set Sha256",
              "type": "string"
            },
            "kind": {
              "const": "coordination",
              "default": "coordination",
              "title": "Kind",
              "type": "string"
            },
            "work": {
              "$ref": "#/$defs/WorkIdentity"
            }
          },
          "required": [
            "branch_id",
            "artifact_selection",
            "work",
            "branch_set_sha256"
          ],
          "title": "CoordinationBranchPlan",
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
        "JoinDeclaration": {
          "additionalProperties": false,
          "description": "One optional exact named-subset join declaration.",
          "properties": {
            "effective_intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "title": "Effective Intent",
              "type": "object"
            },
            "format": {
              "const": "stove0-join-declaration/v1",
              "default": "stove0-join-declaration/v1",
              "title": "Format",
              "type": "string"
            },
            "join_declaration_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Join Declaration Sha256",
              "type": "string"
            },
            "members": {
              "items": {
                "$ref": "#/$defs/JoinMemberDeclaration"
              },
              "minItems": 2,
              "title": "Members",
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
          "title": "JoinDeclaration",
          "type": "object"
        },
        "JoinMemberDeclaration": {
          "additionalProperties": false,
          "description": "Exact named branch and opaque output roles required by the join.",
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Branch Id",
              "type": "string"
            },
            "output_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "title": "Output Roles",
              "type": "array"
            }
          },
          "required": [
            "branch_id",
            "output_roles"
          ],
          "title": "JoinMemberDeclaration",
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
        "PreviewOutcome": {
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
              "anyOf": [
                {
                  "type": "boolean"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Retryable"
            }
          },
          "required": [
            "code",
            "message"
          ],
          "title": "PreviewOutcome",
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
        },
        "WorkflowPlanIntent": {
          "additionalProperties": false,
          "description": "Work-independent fields that deterministically materialize a workflow plan.",
          "properties": {
            "input_retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "title": "Input Retrieval Policy",
              "type": "string"
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
            }
          },
          "required": [
            "operation",
            "target_registration_id",
            "target_contract_sha256"
          ],
          "title": "WorkflowPlanIntent",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "branch_set_plan": {
          "anyOf": [
            {
              "$ref": "#/$defs/BranchSetPlan"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "branch_sets": {
          "default": [],
          "items": {
            "$ref": "#/$defs/BranchSetPlan"
          },
          "title": "Branch Sets",
          "type": "array"
        },
        "format": {
          "const": "stove0-workflow-preview/v1",
          "default": "stove0-workflow-preview/v1",
          "title": "Format",
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
        "outcome": {
          "anyOf": [
            {
              "$ref": "#/$defs/PreviewOutcome"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "preview_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Preview Id",
          "type": "string"
        },
        "preview_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Preview Sha256",
          "type": "string"
        },
        "selections": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ArtifactSelection"
          },
          "title": "Selections",
          "type": "array"
        },
        "state": {
          "enum": [
            "ready",
            "inapplicable",
            "failed",
            "canceled"
          ],
          "title": "State",
          "type": "string"
        },
        "target_plans": {
          "default": [],
          "items": {
            "$ref": "#/$defs/BranchTargetPreview"
          },
          "title": "Target Plans",
          "type": "array"
        },
        "warnings": {
          "default": [],
          "items": {
            "type": "string"
          },
          "title": "Warnings",
          "type": "array"
        },
        "work": {
          "$ref": "#/$defs/WorkIdentity"
        }
      },
      "required": [
        "preview_id",
        "state",
        "work",
        "preview_sha256"
      ],
      "title": "WorkflowPreview",
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-workflow-preview/v1'] = 'stove0-workflow-preview/v1', preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['ready', 'inapplicable', 'failed', 'canceled'], work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan | None = None, branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = (), selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...] = (), target_plans: tuple[stove0_protocol.fork_join.BranchTargetPreview, ...] = (), outcome: stove0_protocol.models.PreviewOutcome | None = None, warnings: tuple[str, ...] = (), preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPreview",
  "unit": "export"
}
```
