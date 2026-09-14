# stove0_operator_contracts.WorkPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workpage:33311faae3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2dd2b8da0b"></a>
- <a id="s-8b7fbfa500"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-6fd93a611c"></a>`module`: `stove0_operator_contracts`
- <a id="s-b3e67ec67c"></a>`name`: `WorkPage`
- <a id="s-447dd88c07"></a>`unit`: `export`

### Declared structure

- <a id="s-edae9587c9"></a>`kind`: `"class"`
- <a id="s-c26cfdc24b"></a>`signature`: `"\"(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken \| None, sort: Literal['updated_at', 'phase', 'work_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], work: tuple[stove0_operator_contracts.WorkView, ...]) -> None\""`

#### Validated model schema

<a id="s-b08a8d0f8d"></a>
- <a id="s-0792c9deff"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-09c4782719"></a>`filters` | yes | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-164b603fbb"></a>`next_page_token` | yes | anyOf=#/$defs/BrowsePageToken \| type="null" |  |
| <a id="s-1debc18ecd"></a>`order` | yes | type="string"; enum=["asc","desc"] |  |
| <a id="s-35306e4609"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-1000225221"></a>`sort` | yes | type="string"; enum=["updated_at","phase","work_id"] |  |
| <a id="s-c8e577bfb7"></a>`work` | yes | type="array"; items=(#/$defs/WorkView) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-208b06065a"></a>`AcceptedTargetJob` | type="object"; fields=`declaration`, `request_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-c5c7727779"></a>`ArtifactDispositionSetIdentity` | type="object"; fields=`disposition_count`, `output_artifact_count`, `output_edge_count`, `sha256`; additional keys=`required` |
| <a id="s-150a1801da"></a>`ArtifactSelectionRef` | type="object"; fields=`artifact_count`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-de978a3ece"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-f96334dc32"></a>`BranchPlan` | type="object"; fields=`artifact_selection`, `branch_id`, `kind`, `workflow_plan`; additional keys=`additionalProperties`, `required` |
| <a id="s-49b578e783"></a>`BranchSetPlan` | type="object"; fields=`branch_set_sha256`, `branches`, `decision_sha256`, `evidence_sha256s`, `format`, `join`, `parent_work`, `retirement_grace_seconds`, `retirement_policy`; additional keys=`additionalProperties`, `required` |
| <a id="s-8cbf22827d"></a>`BranchWorkBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `decision_sha256`, `kind`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-c49e02645f"></a>`BrowsePageToken` | type="string"; minLength=1; maxLength=8192 |
| <a id="s-1d96dcbbf7"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-b17390db18"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-44bea4f2a3"></a>`ControllerEvidence` | type="object"; fields=`controller_evidence_sha256`, `execution_envelope`, `format`; additional keys=`additionalProperties`, `required` |
| <a id="s-e7c9c504b1"></a>`CoordinationBranchPlan` | type="object"; fields=`artifact_selection`, `branch_id`, `branch_set_sha256`, `kind`, `work`; additional keys=`additionalProperties`, `required` |
| <a id="s-37212f95c0"></a>`CoordinationChildSettlementRef` | type="object"; fields=`branch_id`, `kind`, `settlement_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-8d60ad63ea"></a>`CoordinationCollectionResult` | type="object"; fields=`derivation_sha256`, `join_settlement_sha256`, `output_collection`, `output_selection`, `producer_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-3cd778c362"></a>`CoordinationSettlement` | type="object"; fields=`branch_set_sha256`, `children`, `collection_result`, `contains_external_effects`, `final_join_settlement_sha256`, `format`, `settlement_sha256`, `work`; additional keys=`additionalProperties`, `required` |
| <a id="s-40282e1d49"></a>`EffectPlan` | type="object"; fields=`inputs`, `intent`, `observation_result_sha256s`, `operation_contract_sha256`, `operation_id`, `plan_sha256`, `protocol`, `target_contract_sha256`, `target_implementation_id`, `target_options`; additional keys=`additionalProperties`, `required` |
| <a id="s-bbce34584e"></a>`EvaluationBinding` | type="object"; fields=`evaluation_id`, `matrix_sha256`, `parameters`, `variant_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-43438037ba"></a>`ExecutionEnvelope` | type="object"; fields=`claim_id`, `execution_envelope_sha256`, `fence`, `format`, `target_plan`, `workflow_plan`; additional keys=`additionalProperties`, `required` |
| <a id="s-a7307cbf35"></a>`ExternalEffectReceipt` | type="object"; fields=`execution_sha256`, `format`, `job_id`, `operation_contract_sha256`, `plan_sha256`, `receipt_sha256`, `request_sha256`, `result`, `target_contract_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-6119604037"></a>`JoinDeclaration` | type="object"; fields=`effective_intent`, `format`, `join_declaration_sha256`, `members`, `recipe`, `workflow_intent`; additional keys=`additionalProperties`, `required` |
| <a id="s-633723fedd"></a>`JoinInputPlan` | type="object"; fields=`artifact_selection`, `branch_id`, `derivation_sha256`, `output_collection`, `producer_settlement_sha256`, `settlement_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-c91b761cb4"></a>`JoinMemberDeclaration` | type="object"; fields=`branch_id`, `output_roles`; additional keys=`additionalProperties`, `required` |
| <a id="s-7131fac719"></a>`JoinPlan` | type="object"; fields=`branch_set_sha256`, `declaration`, `format`, `inputs`, `join_plan_sha256`, `parent_work_id`, `work`, `workflow_plan`; additional keys=`additionalProperties`, `required` |
| <a id="s-4c5de6ce14"></a>`JoinWorkBinding` | type="object"; fields=`branch_set_sha256`, `kind`, `members`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-5c808e6b0d"></a>`JoinWorkMemberBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `producer_settlement_sha256`, `settlement_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-b1b174c154"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-e69ffb8f42"></a>`JsonValue` | empty object |
| <a id="s-974a0b49dd"></a>`ObservationEvidence` | type="object"; fields=`request`, `result`; additional keys=`additionalProperties`, `required` |
| <a id="s-2b4f70265d"></a>`ObservationFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-6ba44c630f"></a>`ObservationInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-344e016956"></a>`ObservationRequest` | type="object"; fields=`format`, `maximum_result_bytes`, `observer_contract_id`, `observer_contract_sha256`, `observer_descriptor_sha256`, `observer_registration_id`, `options`, `request_id`, `retrieval_policy`, `subjects`, `timeout_seconds`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-653fedbaa6"></a>`ObservationResult` | type="object"; fields=`execution_evidence`, `facts`, `facts_schema`, `facts_sha256`, `failure`, `format`, `inapplicable`, `observer`, `observer_contract_id`, `observer_contract_sha256`, `request_id`, `result_sha256`, `state`, `subjects`; additional keys=`additionalProperties`, `required` |
| <a id="s-4ea959a8e4"></a>`ObserverImplementation` | type="object"; fields=`descriptor_sha256`, `id`, `protocol`, `source_revision`, `version`; additional keys=`additionalProperties`, `required` |
| <a id="s-b94524bd3b"></a>`OperationRef` | type="object"; fields=`id`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-d272ee95e3"></a>`OutputArtifactRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-e582b1dee5"></a>`OutputArtifactSetIdentity` | type="object"; fields=`artifact_count`, `roles`, `sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-b5248b631f"></a>`OutputCollectionRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `derivation_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-44ff8f56f5"></a>`PreviewAcceptanceView` | type="object"; fields=`branch_set_sha256`, `preview_sha256`, `target_plans`; additional keys=`additionalProperties`, `required` |
| <a id="s-e8ea0d4175"></a>`PreviewTargetExpectationView` | type="object"; fields=`branch_id`, `plan_sha256`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-30ef719226"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-999ad9a563"></a>`TargetExecutionEvidence` | type="object"; fields=`execution_sha256`, `operation_contract_sha256`, `plan_sha256`, `runtime`, `target_contract_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-f978fe6e1d"></a>`TargetFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-ebe3418092"></a>`TargetInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-1a92c22544"></a>`TargetInputAuthority` | type="object"; fields=`roles`, `selection`; additional keys=`additionalProperties`, `required` |
| <a id="s-59fa45567d"></a>`TargetInputRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-2b32e80fe5"></a>`TargetJobDeclaration` | type="object"; fields=`claim_id`, `controller_evidence`, `fence`, `job_id`, `plan`, `workspace_assurance`; additional keys=`additionalProperties`, `required` |
| <a id="s-5967cc9caf"></a>`TargetJobStatus` | type="object"; fields=`attempt`, `derivation`, `effect_receipt`, `execution_evidence`, `failure`, `inapplicable`, `job_id`, `output_collection`, `plan_sha256`, `production`, `progress`, `protocol`, `request_sha256`, `state`; allOf=additional keys=`else`, `if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-bb7e7c05be"></a>`TargetOutputBindingSetIdentity` | type="object"; fields=`artifact_count`, `sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-86dbed90c7"></a>`TargetPlanBinding` | type="object"; fields=`operation_contract_sha256`, `plan`, `plan_sha256`, `protocol`, `target_contract_sha256`, `target_implementation_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-147c06beda"></a>`TargetProductionAuthority` | type="object"; fields=`disposition_count`, `disposition_sha256`, `format`, `job_id`, `outputs`, `plan_sha256`, `production_sha256`, `riverhog_disposition_set`, `source_edge_count`, `source_edge_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-8600e54be5"></a>`TargetProgress` | type="object"; fields=`completed`, `phase`, `total`, `unit`; additional keys=`additionalProperties`, `required` |
| <a id="s-03994a2ce6"></a>`TargetSettlementAuthority` | type="object"; fields=`format`, `job_id`, `output_bindings`, `output_collection`, `production_sha256`, `settlement_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-aebe8702e3"></a>`TransformPlan` | type="object"; fields=`inputs`, `intent`, `observation_result_sha256s`, `operation_contract_sha256`, `operation_id`, `plan_sha256`, `protocol`, `target_contract_sha256`, `target_implementation_id`, `target_options`; additional keys=`additionalProperties`, `required` |
| <a id="s-637d0dd0ad"></a>`WorkClaimView` | type="object"; fields=`claim_id`, `fence`; additional keys=`additionalProperties`, `required` |
| <a id="s-28e671ad54"></a>`WorkFailureView` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-81716c1153"></a>`WorkIdentity` | type="object"; fields=`effective_intent`, `evaluation`, `fork_join`, `format`, `inputs`, `recipe`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-009ae10476"></a>`WorkInapplicableView` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-4aa66e2d9c"></a>`WorkView` | type="object"; fields=`abandon_outcome`, `branch_set_plan`, `claim`, `controller_evidence`, `coordination_cancel_requested`, `coordination_settlement`, `expected_target_plan_sha256`, `failure`, `format`, `inapplicable`, `join_plan`, `observation_requests`, `observation_results`, `output`, `phase`, `preview_acceptance`, `retirement_remaining`, `revision`, `target_plan`, `target_request`, `target_settlement`, `target_status`, `work`, `work_id`, `workflow_plan`; additional keys=`additionalProperties`, `required` |
| <a id="s-53509a0132"></a>`WorkflowPlan` | type="object"; fields=`format`, `input_retrieval_policy`, `observations`, `operation`, `output_policy`, `requested_target_options`, `result_kind`, `retirement_grace_seconds`, `retirement_policy`, `target_contract_sha256`, `target_registration_id`, `work`, `workflow_plan_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-38187fd5bb"></a>`WorkflowPlanIntent` | type="object"; fields=`input_retrieval_policy`, `operation`, `output_policy`, `requested_target_options`, `result_kind`, `retirement_grace_seconds`, `retirement_policy`, `target_contract_sha256`, `target_registration_id`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.WorkPage.from_page](stove0-operator-contracts-workpage-from-page.md)

## Governing policies

- <a id="pa-3c28a21596"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c0492a1b9346214b051fcf956d3b04331f218c99895764fe6c6ed7cdbd50afe -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "AcceptedTargetJob": {
          "additionalProperties": false,
          "properties": {
            "declaration": {
              "$ref": "#/$defs/TargetJobDeclaration"
            },
            "request_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "declaration",
            "request_sha256"
          ],
          "type": "object"
        },
        "ArtifactDispositionSetIdentity": {
          "properties": {
            "disposition_count": {
              "type": "integer"
            },
            "output_artifact_count": {
              "type": "integer"
            },
            "output_edge_count": {
              "type": "integer"
            },
            "sha256": {
              "type": "string"
            }
          },
          "required": [
            "disposition_count",
            "output_edge_count",
            "output_artifact_count",
            "sha256"
          ],
          "type": "object"
        },
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
        "ArtifactSubject": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "collection": {
              "$ref": "#/$defs/CollectionRootRef"
            },
            "id": {
              "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
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
              "default": null
            },
            "path": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "role": {
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
            "role",
            "collection",
            "path",
            "bytes",
            "sha256"
          ],
          "type": "object"
        },
        "BranchPlan": {
          "additionalProperties": false,
          "properties": {
            "artifact_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "kind": {
              "const": "leaf",
              "default": "leaf",
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
          "type": "object"
        },
        "BranchSetPlan": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
              "type": "array"
            },
            "decision_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "evidence_sha256s": {
              "default": [],
              "items": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "type": "array"
            },
            "format": {
              "const": "stove0-branch-set/v1",
              "default": "stove0-branch-set/v1",
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
              "type": "integer"
            },
            "retirement_policy": {
              "default": "retain",
              "enum": [
                "retain",
                "retire-after-verified-output"
              ],
              "type": "string"
            }
          },
          "required": [
            "parent_work",
            "decision_sha256",
            "branches",
            "branch_set_sha256"
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
        "BrowsePageToken": {
          "maxLength": 8192,
          "minLength": 1,
          "type": "string"
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
        "ControllerEvidence": {
          "additionalProperties": false,
          "properties": {
            "controller_evidence_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "execution_envelope": {
              "$ref": "#/$defs/ExecutionEnvelope"
            },
            "format": {
              "const": "stove0-controller-evidence/v1",
              "default": "stove0-controller-evidence/v1",
              "type": "string"
            }
          },
          "required": [
            "execution_envelope",
            "controller_evidence_sha256"
          ],
          "type": "object"
        },
        "CoordinationBranchPlan": {
          "additionalProperties": false,
          "properties": {
            "artifact_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "kind": {
              "const": "coordination",
              "default": "coordination",
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
        "CoordinationSettlement": {
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
              "type": "object"
            },
            "observation_result_sha256s": {
              "default": [],
              "items": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "type": "array"
            },
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "protocol": {
              "const": "stove0-effect-target/v1",
              "default": "stove0-effect-target/v1",
              "type": "string"
            },
            "target_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "target_implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "target_options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
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
        "ExecutionEnvelope": {
          "additionalProperties": false,
          "properties": {
            "claim_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "execution_envelope_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "fence": {
              "minimum": 1,
              "type": "integer"
            },
            "format": {
              "const": "stove0-execution-envelope/v1",
              "default": "stove0-execution-envelope/v1",
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
          "type": "object"
        },
        "ExternalEffectReceipt": {
          "additionalProperties": false,
          "properties": {
            "execution_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "format": {
              "const": "stove0-external-effect-receipt/v1",
              "default": "stove0-external-effect-receipt/v1",
              "type": "string"
            },
            "job_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "receipt_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "request_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "result": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object",
              "x-riverhog-encoded-bytes-max": 65536,
              "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-external-effect-receipt"
              }
            },
            "target_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
          "type": "object"
        },
        "JoinDeclaration": {
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
        "JoinInputPlan": {
          "additionalProperties": false,
          "properties": {
            "artifact_selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            },
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "output_collection": {
              "$ref": "#/$defs/CollectionRootRef"
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
            "derivation_sha256",
            "output_collection",
            "artifact_selection"
          ],
          "type": "object"
        },
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
        "JoinPlan": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "declaration": {
              "$ref": "#/$defs/JoinDeclaration"
            },
            "format": {
              "const": "stove0-join-plan/v1",
              "default": "stove0-join-plan/v1",
              "type": "string"
            },
            "inputs": {
              "items": {
                "$ref": "#/$defs/JoinInputPlan"
              },
              "minItems": 2,
              "type": "array"
            },
            "join_plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "parent_work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "work": {
              "$ref": "#/$defs/WorkIdentity"
            },
            "workflow_plan": {
              "$ref": "#/$defs/WorkflowPlan"
            }
          },
          "required": [
            "parent_work_id",
            "branch_set_sha256",
            "declaration",
            "inputs",
            "work",
            "workflow_plan",
            "join_plan_sha256"
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
        "JsonSchemaDocument": {
          "additionalProperties": false,
          "properties": {
            "dialect": {
              "const": "https://json-schema.org/draft/2020-12/schema",
              "default": "https://json-schema.org/draft/2020-12/schema",
              "type": "string"
            },
            "format_policy": {
              "const": "annotation-only",
              "default": "annotation-only",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256",
            "schema"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "ObservationEvidence": {
          "additionalProperties": false,
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
          "type": "object"
        },
        "ObservationFailure": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            },
            "retryable": {
              "type": "boolean"
            }
          },
          "required": [
            "code",
            "message",
            "retryable"
          ],
          "type": "object"
        },
        "ObservationInapplicable": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "code",
            "message"
          ],
          "type": "object"
        },
        "ObservationRequest": {
          "additionalProperties": false,
          "properties": {
            "format": {
              "const": "stove0-observation-request/v1",
              "default": "stove0-observation-request/v1",
              "type": "string"
            },
            "maximum_result_bytes": {
              "default": 1048576,
              "maximum": 67108864,
              "minimum": 1,
              "type": "integer"
            },
            "observer_contract_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "observer_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "observer_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "observer_registration_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
              "type": "string"
            },
            "options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "request_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "type": "string"
            },
            "subjects": {
              "items": {
                "$ref": "#/$defs/ArtifactSubject"
              },
              "minItems": 1,
              "type": "array"
            },
            "timeout_seconds": {
              "default": 300,
              "maximum": 86400,
              "minimum": 1,
              "type": "integer"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
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
          "type": "object"
        },
        "ObservationResult": {
          "additionalProperties": false,
          "properties": {
            "execution_evidence": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
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
              "default": null
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
              "default": null
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
              "type": "string"
            },
            "observer_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "request_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "result_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "state": {
              "enum": [
                "observed",
                "inapplicable",
                "failed",
                "canceled"
              ],
              "type": "string"
            },
            "subjects": {
              "items": {
                "$ref": "#/$defs/ArtifactSubject"
              },
              "minItems": 1,
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
          "type": "object"
        },
        "ObserverImplementation": {
          "additionalProperties": false,
          "properties": {
            "descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "protocol": {
              "const": "stove0-content-observer/v1",
              "default": "stove0-content-observer/v1",
              "type": "string"
            },
            "source_revision": {
              "maxLength": 200,
              "minLength": 1,
              "type": "string"
            },
            "version": {
              "maxLength": 120,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "id",
            "version",
            "source_revision",
            "descriptor_sha256"
          ],
          "type": "object"
        },
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
        "OutputArtifactRoleCount": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role",
            "count"
          ],
          "type": "object"
        },
        "OutputArtifactSetIdentity": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
            },
            "roles": {
              "items": {
                "$ref": "#/$defs/OutputArtifactRoleCount"
              },
              "minItems": 1,
              "type": "array"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "artifact_count",
            "total_bytes",
            "roles",
            "sha256"
          ],
          "type": "object"
        },
        "OutputCollectionRef": {
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
            },
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity",
            "derivation_sha256"
          ],
          "type": "object"
        },
        "PreviewAcceptanceView": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "preview_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "target_plans": {
              "items": {
                "$ref": "#/$defs/PreviewTargetExpectationView"
              },
              "type": "array"
            }
          },
          "required": [
            "preview_sha256",
            "branch_set_sha256",
            "target_plans"
          ],
          "type": "object"
        },
        "PreviewTargetExpectationView": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "branch_id",
            "work_id",
            "plan_sha256"
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
        "TargetExecutionEvidence": {
          "additionalProperties": false,
          "properties": {
            "execution_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "runtime": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "target_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "target_contract_sha256",
            "operation_contract_sha256",
            "plan_sha256",
            "execution_sha256"
          ],
          "type": "object"
        },
        "TargetFailure": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            },
            "retryable": {
              "type": "boolean"
            }
          },
          "required": [
            "code",
            "message",
            "retryable"
          ],
          "type": "object"
        },
        "TargetInapplicable": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "code",
            "message"
          ],
          "type": "object"
        },
        "TargetInputAuthority": {
          "additionalProperties": false,
          "properties": {
            "roles": {
              "items": {
                "$ref": "#/$defs/TargetInputRoleCount"
              },
              "minItems": 1,
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
          "type": "object"
        },
        "TargetInputRoleCount": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role",
            "count"
          ],
          "type": "object"
        },
        "TargetJobDeclaration": {
          "additionalProperties": false,
          "properties": {
            "claim_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "controller_evidence": {
              "$ref": "#/$defs/ControllerEvidence"
            },
            "fence": {
              "minimum": 1,
              "type": "integer"
            },
            "job_id": {
              "pattern": "^[0-9a-f]{64}$",
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
              ]
            },
            "workspace_assurance": {
              "enum": [
                "encrypted",
                "ephemeral"
              ],
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
          "type": "object"
        },
        "TargetJobStatus": {
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
              "default": null
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
              "type": "string"
            },
            "request_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
          "type": "object"
        },
        "TargetOutputBindingSetIdentity": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "artifact_count",
            "total_bytes",
            "sha256"
          ],
          "type": "object"
        },
        "TargetPlanBinding": {
          "additionalProperties": false,
          "properties": {
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "plan": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "protocol": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "target_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "target_implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
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
          "type": "object"
        },
        "TargetProductionAuthority": {
          "additionalProperties": false,
          "properties": {
            "disposition_count": {
              "minimum": 1,
              "type": "integer"
            },
            "disposition_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "format": {
              "const": "stove0-target-production/v1",
              "default": "stove0-target-production/v1",
              "type": "string"
            },
            "job_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "outputs": {
              "$ref": "#/$defs/OutputArtifactSetIdentity"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "production_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "riverhog_disposition_set": {
              "$ref": "#/$defs/ArtifactDispositionSetIdentity"
            },
            "source_edge_count": {
              "minimum": 1,
              "type": "integer"
            },
            "source_edge_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
          "type": "object"
        },
        "TargetProgress": {
          "additionalProperties": false,
          "properties": {
            "completed": {
              "minimum": 0,
              "type": "integer"
            },
            "phase": {
              "maxLength": 120,
              "minLength": 1,
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
              "default": null
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
              "default": null
            }
          },
          "required": [
            "phase",
            "completed"
          ],
          "type": "object"
        },
        "TargetSettlementAuthority": {
          "additionalProperties": false,
          "properties": {
            "format": {
              "const": "stove0-target-settlement/v1",
              "default": "stove0-target-settlement/v1",
              "type": "string"
            },
            "job_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "output_bindings": {
              "$ref": "#/$defs/TargetOutputBindingSetIdentity"
            },
            "output_collection": {
              "$ref": "#/$defs/OutputCollectionRef"
            },
            "production_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "settlement_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "job_id",
            "production_sha256",
            "output_collection",
            "output_bindings",
            "settlement_sha256"
          ],
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
              "type": "object"
            },
            "observation_result_sha256s": {
              "default": [],
              "items": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "type": "array"
            },
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "protocol": {
              "const": "stove0-transform-target/v1",
              "default": "stove0-transform-target/v1",
              "type": "string"
            },
            "target_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "target_implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "target_options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
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
          "type": "object"
        },
        "WorkClaimView": {
          "additionalProperties": false,
          "properties": {
            "claim_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "fence": {
              "minimum": 1,
              "type": "integer"
            }
          },
          "required": [
            "claim_id",
            "fence"
          ],
          "type": "object"
        },
        "WorkFailureView": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            },
            "retryable": {
              "type": "boolean"
            }
          },
          "required": [
            "code",
            "message",
            "retryable"
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
        },
        "WorkInapplicableView": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "code",
            "message"
          ],
          "type": "object"
        },
        "WorkView": {
          "additionalProperties": false,
          "properties": {
            "abandon_outcome": {
              "anyOf": [
                {
                  "enum": [
                    "inapplicable",
                    "failed",
                    "canceled"
                  ],
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
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
            "claim": {
              "anyOf": [
                {
                  "$ref": "#/$defs/WorkClaimView"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "controller_evidence": {
              "anyOf": [
                {
                  "$ref": "#/$defs/ControllerEvidence"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "coordination_cancel_requested": {
              "default": false,
              "type": "boolean"
            },
            "coordination_settlement": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CoordinationSettlement"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "expected_target_plan_sha256": {
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
            "failure": {
              "anyOf": [
                {
                  "$ref": "#/$defs/WorkFailureView"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "format": {
              "const": "stove0-work-view/v1",
              "default": "stove0-work-view/v1",
              "type": "string"
            },
            "inapplicable": {
              "anyOf": [
                {
                  "$ref": "#/$defs/WorkInapplicableView"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "join_plan": {
              "anyOf": [
                {
                  "$ref": "#/$defs/JoinPlan"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "observation_requests": {
              "default": [],
              "items": {
                "$ref": "#/$defs/ObservationRequest"
              },
              "type": "array"
            },
            "observation_results": {
              "default": [],
              "items": {
                "$ref": "#/$defs/ObservationResult"
              },
              "type": "array"
            },
            "output": {
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
            "phase": {
              "enum": [
                "eligible",
                "claimed",
                "observing",
                "planning",
                "target_preflight",
                "queued",
                "executing",
                "output_finalizing",
                "verifying",
                "settled",
                "retirement_pending",
                "coordinating",
                "abandon_pending",
                "complete",
                "inapplicable",
                "failed",
                "canceled"
              ],
              "type": "string"
            },
            "preview_acceptance": {
              "anyOf": [
                {
                  "$ref": "#/$defs/PreviewAcceptanceView"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "retirement_remaining": {
              "default": [],
              "items": {
                "type": "integer"
              },
              "type": "array"
            },
            "revision": {
              "minimum": 1,
              "type": "integer"
            },
            "target_plan": {
              "anyOf": [
                {
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
                  ]
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "target_request": {
              "anyOf": [
                {
                  "$ref": "#/$defs/AcceptedTargetJob"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "target_settlement": {
              "anyOf": [
                {
                  "$ref": "#/$defs/TargetSettlementAuthority"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "target_status": {
              "anyOf": [
                {
                  "$ref": "#/$defs/TargetJobStatus"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "work": {
              "$ref": "#/$defs/WorkIdentity"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "workflow_plan": {
              "anyOf": [
                {
                  "$ref": "#/$defs/WorkflowPlan"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            }
          },
          "required": [
            "work_id",
            "work",
            "phase",
            "revision"
          ],
          "type": "object"
        },
        "WorkflowPlan": {
          "additionalProperties": false,
          "properties": {
            "format": {
              "const": "stove0-workflow-plan/v1",
              "default": "stove0-workflow-plan/v1",
              "type": "string"
            },
            "input_retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "type": "string"
            },
            "observations": {
              "default": [],
              "items": {
                "$ref": "#/$defs/ObservationEvidence"
              },
              "type": "array"
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
            },
            "work": {
              "$ref": "#/$defs/WorkIdentity"
            },
            "workflow_plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
        "filters": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
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
          "enum": [
            "asc",
            "desc"
          ],
          "type": "string"
        },
        "page_size": {
          "maximum": 100,
          "minimum": 1,
          "type": "integer"
        },
        "sort": {
          "enum": [
            "updated_at",
            "phase",
            "work_id"
          ],
          "type": "string"
        },
        "work": {
          "items": {
            "$ref": "#/$defs/WorkView"
          },
          "type": "array"
        }
      },
      "required": [
        "page_size",
        "next_page_token",
        "sort",
        "order",
        "filters",
        "work"
      ],
      "type": "object"
    },
    "signature": "\"(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, sort: Literal['updated_at', 'phase', 'work_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], work: tuple[stove0_operator_contracts.WorkView, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkPage",
  "unit": "export"
}
```
