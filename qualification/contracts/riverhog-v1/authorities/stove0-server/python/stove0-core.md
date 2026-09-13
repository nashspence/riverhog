# stove0_core

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core:d865a69055 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-16c6472aba"></a>
| Field | Shape |
|---|---|
| <a id="s-d15bf522ed"></a>`candidate_id` | "python:stove0-server:stove0_core" |
| <a id="s-fdb773b296"></a>`distribution` | "stove0-server" |
| <a id="s-96df2cd3f8"></a>`exports` | additional keys=`AbandonOutcome`, `ClaimBinding`, `ClassificationAdmissionService`, `ConcurrentEvaluationUpdate`, `ConcurrentWorkUpdate`, `CoordinationProjection`, `DEFAULT_OPERATIONAL_STATE_RETENTION_SECONDS`, `EndpointRegistration`, `EvaluationChild`, `EvaluationChildState`, `EvaluationPhase`, `EvaluationRecord`, `EvaluationReview`, `EvaluationService`, `EvaluationStore`, `EvaluationWorkController`, `HttpObserverPort`, `HttpTargetPort`, `InMemoryEvaluationStore`, `InMemoryWorkStore`, `ObserverPort`, `ParentOutcomeBinding`, `PlanningPort`, `PreviewAcceptance`, `PreviewRiverhogPort`, `PreviewTargetExpectation`, `RecipeCatalog`, `RecipeDefinition`, `RecipePlanner`, `RiverhogApi`, `RiverhogControlPort`, `SchedulerRole`, `SqlAlchemyStateStore`, `Stove0Coordinator`, `Stove0RiverhogClient`, `Stove0RuntimeConfig`, `Stove0Scheduler`, `Stove0StateError`, `Stove0WorkService`, `TargetCallbackAuthority`, `TargetInvocationAuthority`, `TargetPort`, `TerminalPhase`, `WorkFailure`, `WorkInapplicable`, `WorkPhase`, `WorkRecord`, `WorkStore`, `WorkflowPreviewService`, `database_url_from_environment`, `project_coordination`, `scheduler_role`, `stove0_state_schema` |
| <a id="s-315827c7ff"></a>`module` | "stove0_core" |

## Governing policies

- <a id="pa-841bb86e31"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/58`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74aa485974c5017d92f5e38d301dffea8041f8860f704ccc92d52419b1b4c2f4 -->

```json
{
  "candidate_id": "python:stove0-server:stove0_core",
  "distribution": "stove0-server",
  "exports": {
    "AbandonOutcome": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "ClaimBinding": {
      "kind": "class",
      "schema_sha256": "659936b8015faa931dac1f67ac09090a862453b08c01b2b835ee6c759c8b2a58",
      "signature": "'(*, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)]) -> None'"
    },
    "ClassificationAdmissionService": {
      "kind": "class",
      "members": {
        "advance": {
          "kind": "method",
          "signature": "\"(self, *, limit: 'int' = 25) -> 'AdmissionRun'\""
        },
        "get_admission": {
          "kind": "method",
          "signature": "\"(self, admission_id: 'str') -> 'AdmissionView'\""
        },
        "list_admissions": {
          "kind": "method",
          "signature": "\"(self, *, page_size: 'int', position: 'tuple[str | int | bool | bytes | None, ...] | None', policy_id: 'str | None', state: 'AdmissionState | None', query: 'str | None', sort: 'AdmissionSort', order: 'SortOrder') -> 'dict[str, object]'\""
        },
        "policies": {
          "kind": "method",
          "signature": "\"(self) -> 'AdmissionPolicyCatalogView'\""
        },
        "rebaseline": {
          "kind": "method",
          "signature": "'(self, policy_id: \\'str\\', *, mode: \"Literal[\\'observe\\', \\'backfill\\']\" = \\'observe\\') -> \\'AdmissionPolicyStatus\\''"
        }
      },
      "signature": "\"(*, catalog: 'AdmissionCatalog', riverhog: 'ApiClient', state: 'SqlAlchemyStateStore', planner: 'RecipePlanner', preview: 'WorkflowPreviewService', coordinator: 'Stove0Coordinator') -> 'None'\""
    },
    "ConcurrentEvaluationUpdate": {
      "kind": "class",
      "signature": "unavailable"
    },
    "ConcurrentWorkUpdate": {
      "kind": "class",
      "signature": "unavailable"
    },
    "CoordinationProjection": {
      "fields": [
        {
          "default": "required",
          "name": "evaluation",
          "type": "'BranchSetEvaluation'"
        },
        {
          "default": "required",
          "name": "selection_documents",
          "type": "'dict[str, ArtifactSelection]'"
        },
        {
          "default": "None",
          "name": "pending_join",
          "type": "'JoinPlan | None'"
        },
        {
          "default": "()",
          "name": "pending_join_selections",
          "type": "'tuple[ArtifactSelection, ...]'"
        }
      ],
      "kind": "class",
      "signature": "\"(evaluation: 'BranchSetEvaluation', selection_documents: 'dict[str, ArtifactSelection]', pending_join: 'JoinPlan | None' = None, pending_join_selections: 'tuple[ArtifactSelection, ...]' = ()) -> None\""
    },
    "DEFAULT_OPERATIONAL_STATE_RETENTION_SECONDS": {
      "kind": "constant",
      "value": 2592000
    },
    "EndpointRegistration": {
      "fields": [
        {
          "default": "required",
          "name": "base_url",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "token",
          "type": "'str | None'"
        },
        {
          "default": "required",
          "name": "allow_insecure_http",
          "type": "'bool'"
        },
        {
          "default": "()",
          "name": "semantic_validator_providers",
          "type": "'tuple[str, ...]'"
        }
      ],
      "kind": "class",
      "signature": "\"(base_url: 'str', token: 'str | None', allow_insecure_http: 'bool', semantic_validator_providers: 'tuple[str, ...]' = ()) -> None\""
    },
    "EvaluationChild": {
      "kind": "class",
      "members": {
        "validate_output": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "1b83214578ce9b5a5a7e03e493454b0f3b2ad79ad49a24cc8885e4ca80265a0e",
      "signature": "\"(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['pending', 'active', 'complete', 'inapplicable', 'failed', 'canceled'] = 'pending', output: stove0_target_protocol.protocol.OutputCollectionRef | None = None) -> None\""
    },
    "EvaluationChildState": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "EvaluationPhase": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "EvaluationRecord": {
      "kind": "class",
      "members": {
        "evaluation_id": {
          "kind": "property",
          "signature": "\"(self) -> 'str'\""
        },
        "validate_children": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "ab8b675f9648345599ff6e54ee4ad1a358dcd426ae98766419ced7bb2b78339d",
      "signature": "\"(*, format: Literal['stove0-evaluation-record/v1'] = 'stove0-evaluation-record/v1', definition: stove0_protocol.models.EvaluationDefinition, phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled'] = 'planning', revision: Annotated[int, Ge(ge=1)] = 1, children: tuple[stove0_core.evaluation.EvaluationChild, ...], reviews: tuple[stove0_core.evaluation.EvaluationReview, ...] = ()) -> None\""
    },
    "EvaluationReview": {
      "kind": "class",
      "members": {
        "meaningful": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "c7ca0c39feb6bf6039a1307a6d0b77476adef3c5968e2de4fba9e0a9c646332c",
      "signature": "'(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], rating: Annotated[int | None, Ge(ge=1), Le(le=5)] = None, note: Annotated[str | None, MaxLen(max_length=4000)] = None, updated_by: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None'"
    },
    "EvaluationService": {
      "kind": "class",
      "members": {
        "cancel": {
          "kind": "method",
          "signature": "\"(self, evaluation_id: 'str', *, controller: 'EvaluationWorkController') -> 'EvaluationRecord'\""
        },
        "create_or_resume": {
          "kind": "method",
          "signature": "\"(self, definition: 'EvaluationDefinition') -> 'EvaluationRecord'\""
        },
        "refresh": {
          "kind": "method",
          "signature": "\"(self, evaluation_id: 'str') -> 'EvaluationRecord'\""
        },
        "retry_failed": {
          "kind": "method",
          "signature": "\"(self, evaluation_id: 'str', variant_id: 'str', *, controller: 'EvaluationWorkController') -> 'EvaluationRecord'\""
        },
        "review": {
          "kind": "method",
          "signature": "\"(self, evaluation_id: 'str', review: 'EvaluationReview') -> 'EvaluationRecord'\""
        },
        "step": {
          "kind": "method",
          "signature": "\"(self, evaluation_id: 'str', *, controller: 'EvaluationWorkController') -> 'EvaluationRecord'\""
        }
      },
      "signature": "\"(store: 'EvaluationStore', *, work: 'Stove0WorkService') -> 'None'\""
    },
    "EvaluationStore": {
      "kind": "class",
      "members": {
        "compare_and_swap": {
          "kind": "method",
          "signature": "\"(self, evaluation_id: 'str', *, expected_revision: 'int', replacement: 'EvaluationRecord') -> 'EvaluationRecord'\""
        },
        "create": {
          "kind": "method",
          "signature": "\"(self, record: 'EvaluationRecord') -> 'EvaluationRecord'\""
        },
        "load": {
          "kind": "method",
          "signature": "\"(self, evaluation_id: 'str') -> 'EvaluationRecord | None'\""
        }
      },
      "signature": "'(*args, **kwargs)'"
    },
    "EvaluationWorkController": {
      "kind": "class",
      "members": {
        "cancel": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str') -> 'WorkRecord'\""
        },
        "retry": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str') -> 'WorkRecord'\""
        },
        "step": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str') -> 'WorkRecord'\""
        }
      },
      "signature": "'(*args, **kwargs)'"
    },
    "HttpObserverPort": {
      "kind": "class",
      "members": {
        "descriptor": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str') -> 'ObserverDescriptor'\""
        },
        "observe": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str', invocation: 'ObservationInvocation', *, descriptor: 'ObserverDescriptor') -> 'ObservationResult'\""
        }
      },
      "signature": "\"(registrations: 'dict[str, ContentObserverClient]') -> 'None'\""
    },
    "HttpTargetPort": {
      "kind": "class",
      "members": {
        "cancel_job": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str', request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
        },
        "contract": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str') -> 'TargetContract'\""
        },
        "get_job": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str', request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
        },
        "preflight": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str', request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
        },
        "put_job": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str', request: 'TargetJobRequest', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
        }
      },
      "signature": "\"(registrations: 'dict[str, TargetClient]') -> 'None'\""
    },
    "InMemoryEvaluationStore": {
      "kind": "class",
      "members": {
        "compare_and_swap": {
          "kind": "method",
          "signature": "\"(self, evaluation_id: 'str', *, expected_revision: 'int', replacement: 'EvaluationRecord') -> 'EvaluationRecord'\""
        },
        "create": {
          "kind": "method",
          "signature": "\"(self, record: 'EvaluationRecord') -> 'EvaluationRecord'\""
        },
        "load": {
          "kind": "method",
          "signature": "\"(self, evaluation_id: 'str') -> 'EvaluationRecord | None'\""
        }
      },
      "signature": "\"() -> 'None'\""
    },
    "InMemoryWorkStore": {
      "kind": "class",
      "members": {
        "admit_branch_set": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', decision: 'BranchSetDecision') -> 'WorkRecord'\""
        },
        "admit_join": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', plan: 'JoinPlan', selections: 'Sequence[ArtifactSelection]') -> 'WorkRecord'\""
        },
        "compare_and_swap": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', replacement: 'WorkRecord') -> 'WorkRecord'\""
        },
        "compare_and_swap_target_production_seal": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetProductionSealRecord') -> 'TargetProductionSealRecord'\""
        },
        "compare_and_swap_target_settlement_seal": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""
        },
        "create": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord') -> 'WorkRecord'\""
        },
        "ensure_target_production_receiving": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord'\""
        },
        "ensure_target_settlement_binding": {
          "kind": "method",
          "signature": "\"(self, record: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""
        },
        "iter_selection_artifacts": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str') -> 'Iterator[ArtifactSubject]'\""
        },
        "iter_target_dispositions": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[InputDispositionDeclaration]'\""
        },
        "iter_target_outputs": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""
        },
        "iter_target_outputs_by_path": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""
        },
        "iter_target_source_edges": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""
        },
        "iter_target_source_edges_by_input": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""
        },
        "load": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str') -> 'WorkRecord | None'\""
        },
        "load_selection": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str') -> 'ArtifactSelection | None'\""
        },
        "load_selection_artifact": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str', artifact_id: 'str') -> 'ArtifactSubject | None'\""
        },
        "load_selection_ref": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str') -> 'ArtifactSelectionRef | None'\""
        },
        "load_target_disposition": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', input_id: 'str') -> 'InputDispositionDeclaration | None'\""
        },
        "load_target_output": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', output_id: 'str') -> 'OutputArtifact | None'\""
        },
        "load_target_production_seal": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord | None'\""
        },
        "load_target_settlement_seal": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetSettlementSealRecord | None'\""
        },
        "record_target_disposition": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'None'\""
        },
        "record_target_output": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', output: 'OutputArtifact') -> 'None'\""
        },
        "record_target_source_edge": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', edge: 'OutputSourceEdge') -> 'None'\""
        },
        "retain_selection": {
          "kind": "method",
          "signature": "\"(self, selection: 'ArtifactSelection') -> 'None'\""
        },
        "scan_target_production_seals": {
          "kind": "method",
          "signature": "\"(self, *, state: 'TargetProductionSealState', limit: 'int') -> 'tuple[TargetProductionSealRecord, ...]'\""
        },
        "selection_artifact_page": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str', *, continuation: 'str | None', limit: 'int') -> 'tuple[tuple[ArtifactSubject, ...], str | None, bool]'\""
        },
        "target_disposition_page": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str | None', limit: 'int') -> 'tuple[InputDispositionDeclaration, ...]'\""
        },
        "target_output_page": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str | None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""
        },
        "target_output_path_page": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_path: 'str | None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""
        },
        "target_source_edge_page": {
          "kind": "method",
          "signature": "'(self, work_id: \\'str\\', job_id: \\'str\\', *, order: \"Literal[\\'output\\', \\'input\\']\", after_output_id: \\'str | None\\', after_input_id: \\'str | None\\', limit: \\'int\\') -> \\'tuple[OutputSourceEdge, ...]\\''"
        }
      },
      "signature": "\"() -> 'None'\""
    },
    "ObserverPort": {
      "kind": "class",
      "members": {
        "descriptor": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str') -> 'ObserverDescriptor'\""
        },
        "observe": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str', invocation: 'ObservationInvocation', *, descriptor: 'ObserverDescriptor') -> 'ObservationResult'\""
        }
      },
      "signature": "'(*args, **kwargs)'"
    },
    "ParentOutcomeBinding": {
      "fields": [
        {
          "default": "required",
          "name": "claim",
          "type": "'ClaimBinding'"
        },
        {
          "default": "required",
          "name": "outcome_id",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "signature": "\"(claim: 'ClaimBinding', outcome_id: 'str') -> None\""
    },
    "PlanningPort": {
      "kind": "class",
      "members": {
        "observation_requests": {
          "kind": "method",
          "signature": "\"(self, work: 'WorkIdentity') -> 'tuple[ObservationRequest, ...]'\""
        },
        "operation_contract": {
          "kind": "method",
          "signature": "\"(self, operation: 'OperationRef') -> 'OperationContract'\""
        },
        "target_input_selection": {
          "kind": "method",
          "signature": "\"(self, plan: 'WorkflowPlan', selections: 'dict[str, ArtifactSelection]') -> 'ArtifactSelection'\""
        },
        "target_preflight_request": {
          "kind": "method",
          "signature": "\"(self, plan: 'WorkflowPlan', selections: 'dict[str, ArtifactSelection]') -> 'TargetPreflightRequest'\""
        },
        "workflow_plan": {
          "kind": "method",
          "signature": "\"(self, work: 'WorkIdentity', observations: 'tuple[ObservationEvidence, ...]', *, nested_observer: 'Callable[[WorkIdentity], tuple[ObservationEvidence, ...]] | None' = None) -> 'BranchSetDecision | WorkInapplicable'\""
        }
      },
      "signature": "'(*args, **kwargs)'"
    },
    "PreviewAcceptance": {
      "kind": "class",
      "members": {
        "canonical_targets": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        },
        "from_preview": {
          "kind": "classmethod",
          "signature": "\"(cls, preview: 'WorkflowPreview') -> 'PreviewAcceptance'\""
        }
      },
      "schema_sha256": "20a99f60eb42afdb3b4dcf73b8a47e422e2b26c60fdb64e420990ff78c34da8c",
      "signature": "\"(*, preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_plans: tuple[stove0_core.work_state.PreviewTargetExpectation, ...]) -> None\""
    },
    "PreviewRiverhogPort": {
      "kind": "class",
      "members": {
        "abandon_preview_claim": {
          "kind": "method",
          "signature": "\"(self, request: 'WorkflowPreviewRequest', claim: 'ClaimBinding') -> 'None'\""
        },
        "acquire_preview_claim": {
          "kind": "method",
          "signature": "\"(self, request: 'WorkflowPreviewRequest') -> 'ClaimBinding'\""
        },
        "observation_authority": {
          "kind": "method",
          "signature": "\"(self, claim: 'ClaimBinding', request: 'ObservationRequest') -> 'ObserverRuntimeAuthority'\""
        }
      },
      "signature": "'(*args, **kwargs)'"
    },
    "PreviewTargetExpectation": {
      "kind": "class",
      "schema_sha256": "5a61cd78514147734c19e5793fffabbafdb2c06e87e48d8a737ef672985c25ac",
      "signature": "\"(*, branch_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
    },
    "RecipeCatalog": {
      "kind": "class",
      "members": {
        "load": {
          "kind": "classmethod",
          "signature": "\"(cls, path: 'Path') -> 'RecipeCatalog'\""
        },
        "operation": {
          "kind": "method",
          "signature": "\"(self, operation_id: 'str') -> 'OperationContract'\""
        },
        "recipe": {
          "kind": "method",
          "signature": "\"(self, recipe_id: 'str', revision: 'int | None' = None) -> 'RecipeDefinition'\""
        },
        "sha256": {
          "kind": "property",
          "signature": "\"(self) -> 'str'\""
        },
        "valid_catalog": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        },
        "validation_document": {
          "kind": "method",
          "signature": "\"(self) -> 'dict[str, JsonValue]'\""
        }
      },
      "schema_sha256": "d917d4c47bebfc8abcea36832e60bdf80ca2ac7cd717d0f30305b3d377409fd5",
      "signature": "\"(*, format: Literal['stove0-recipes/v1'] = 'stove0-recipes/v1', operations: tuple[stove0_target_protocol.protocol.OperationContract, ...], recipes: tuple[stove0_recipe_config.models.RecipeDefinition, ...]) -> None\""
    },
    "RecipeDefinition": {
      "kind": "class",
      "members": {
        "canonical_members": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        },
        "identity_document": {
          "kind": "method",
          "signature": "\"(self) -> 'dict[str, JsonValue]'\""
        },
        "ref": {
          "kind": "property",
          "signature": "\"(self) -> 'RecipeRef'\""
        },
        "sha256": {
          "kind": "property",
          "signature": "\"(self) -> 'str'\""
        }
      },
      "schema_sha256": "6c28d2c8a41f504854067b2123a03ac0343f7c20f4281141a0871f4e6e4deb50",
      "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[int, Ge(ge=1)], event_input_closure: Literal['single-finalized-collection'] = 'single-finalized-collection', artifact_associations: tuple[stove0_recipe_config.models.ArtifactAssociation, ...] = (), observers: tuple[stove0_recipe_config.models.ObserverUse, ...] = (), routes: Annotated[tuple[Annotated[stove0_recipe_config.models.RecipeRoute | stove0_recipe_config.models.RecipeCoordinationRoute, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], unmatched_artifact_disposition: Literal['retain-in-source', 'reject-work'], allow_derived_inputs: bool = False, source_retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, join: stove0_recipe_config.models.RecipeJoin | None = None) -> None\""
    },
    "RecipePlanner": {
      "kind": "class",
      "members": {
        "create_work": {
          "kind": "method",
          "signature": "\"(self, recipe_id: 'str', roots: 'Sequence[CollectionRootRef]', *, revision: 'int | None' = None, effective_intent: 'Mapping[str, JsonValue] | None' = None) -> 'WorkIdentity'\""
        },
        "observation_requests": {
          "kind": "method",
          "signature": "\"(self, work: 'WorkIdentity') -> 'tuple[ObservationRequest, ...]'\""
        },
        "operation_contract": {
          "kind": "method",
          "signature": "\"(self, operation: 'OperationRef') -> 'OperationContract'\""
        },
        "target_input_selection": {
          "kind": "method",
          "signature": "\"(self, plan: 'WorkflowPlan', selections: 'Mapping[str, ArtifactSelection]') -> 'ArtifactSelection'\""
        },
        "target_preflight_request": {
          "kind": "method",
          "signature": "\"(self, plan: 'WorkflowPlan', selections: 'Mapping[str, ArtifactSelection]') -> 'TargetPreflightRequest'\""
        },
        "workflow_plan": {
          "kind": "method",
          "signature": "\"(self, work: 'WorkIdentity', observations: 'tuple[ObservationEvidence, ...]', *, nested_observer: 'NestedObservation | None' = None) -> 'BranchSetDecision | WorkInapplicable'\""
        }
      },
      "signature": "\"(*, catalog: 'RecipeCatalog', riverhog: 'ApiClient', observers: 'ObserverPort', targets: 'TargetPort') -> 'None'\""
    },
    "RiverhogApi": {
      "kind": "class",
      "members": {
        "abandon_processing_claim": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, fence: 'int', reason: 'str') -> 'ProcessingClaimDocument'\""
        },
        "begin_processing_claim_retirement": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, fence: 'int') -> 'ProcessingClaimDocument'\""
        },
        "create_or_resume_processing_claim": {
          "kind": "method",
          "signature": "\"(self, *, work_id: 'str', work_document: 'Mapping[str, Any]', work_document_sha256: 'str', inputs: 'Iterable[Mapping[str, Any]]', lease_seconds: 'int' = 1800, purpose: 'str' = 'collection-work/v1') -> 'ProcessingClaimDocument'\""
        },
        "create_transform_capability": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, fence: 'int', audience: 'str', actions: 'Sequence[CapabilityAction]' = ('read-inputs',), artifacts: 'Iterable[Mapping[str, Any]]', ttl_seconds: 'int' = 900) -> 'TransformCapabilityDocument'\""
        },
        "delete_collection": {
          "kind": "method",
          "signature": "\"(self, collection_id: 'int', *, challenge: 'str', retirement_claim_id: 'str | None' = None, event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'\""
        },
        "get_collection": {
          "kind": "method",
          "signature": "\"(self, collection_id: 'int') -> 'dict[str, Any]'\""
        },
        "get_collection_derivation": {
          "kind": "method",
          "signature": "\"(self, collection_id: 'int') -> 'CollectionDerivationResponseDocument'\""
        },
        "get_portable_collection_inventory": {
          "kind": "method",
          "signature": "\"(self, collection_id: 'int', *, cursor: 'str | None' = None, limit: 'int' = 100, inventory_identity: 'str | None' = None) -> 'PortableCollectionInventoryPage'\""
        },
        "get_processing_claim": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str') -> 'ProcessingClaimDocument'\""
        },
        "get_processing_claim_dispositions": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str') -> 'ArtifactDispositionSetDocument'\""
        },
        "list_processing_claim_disposition_outputs": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'ArtifactDispositionOutputPageDocument'\""
        },
        "list_processing_claim_dispositions": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'ArtifactDispositionPageDocument'\""
        },
        "list_processing_claim_outcomes": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'ProcessingOutcomePageDocument'\""
        },
        "plan_collection_deletion": {
          "kind": "method",
          "signature": "\"(self, collection_id: 'int', *, retirement_claim_id: 'str | None' = None) -> 'dict[str, Any]'\""
        },
        "record_processing_claim_disposition_outputs": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, fence: 'int', outputs: 'Sequence[Mapping[str, Any]]') -> 'ArtifactDispositionSetDocument'\""
        },
        "record_processing_claim_dispositions": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, fence: 'int', dispositions: 'Sequence[Mapping[str, Any]]') -> 'ArtifactDispositionSetDocument'\""
        },
        "release_processing_claim": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, fence: 'int') -> 'ProcessingClaimDocument'\""
        },
        "renew_processing_claim": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, fence: 'int', lease_seconds: 'int' = 1800) -> 'ProcessingClaimDocument'\""
        },
        "restart_processing_claim": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, fence: 'int', lease_seconds: 'int' = 1800) -> 'ProcessingClaimDocument'\""
        },
        "seal_processing_claim_dispositions": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, fence: 'int') -> 'ArtifactDispositionSetDocument'\""
        },
        "seal_processing_claim_plan": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, fence: 'int', execution_id: 'str', controller_evidence: 'Mapping[str, Any]', controller_evidence_sha256: 'str', operation_id: 'str', operation_sha256: 'str', input_artifacts: 'Iterable[Mapping[str, Any]]', retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0) -> 'ProcessingClaimDocument'\""
        },
        "settle_processing_claim": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, fence: 'int', output_collection_id: 'int', derivation: 'Mapping[str, Any]', outcome_claim_id: 'str | None' = None, outcome_fence: 'int | None' = None, outcome_id: 'str | None' = None) -> 'ProcessingClaimDocument'\""
        },
        "settle_processing_claim_outcomes": {
          "kind": "method",
          "signature": "\"(self, claim_id: 'str', *, fence: 'int', retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0) -> 'ProcessingClaimDocument'\""
        }
      },
      "signature": "'(*args, **kwargs)'"
    },
    "RiverhogControlPort": {
      "kind": "class",
      "members": {
        "abandon_claim": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord') -> 'None'\""
        },
        "acquire_claim": {
          "kind": "method",
          "signature": "\"(self, work: 'WorkIdentity') -> 'ClaimBinding'\""
        },
        "begin_retirement": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord') -> 'bool'\""
        },
        "observation_authority": {
          "kind": "method",
          "signature": "\"(self, claim: 'ClaimBinding', request: 'ObservationRequest') -> 'ObserverRuntimeAuthority'\""
        },
        "release_claim": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord') -> 'None'\""
        },
        "renew_claim": {
          "kind": "method",
          "signature": "\"(self, work: 'WorkIdentity', claim: 'ClaimBinding') -> 'ClaimBinding'\""
        },
        "restart_claim": {
          "kind": "method",
          "signature": "\"(self, work: 'WorkIdentity', claim: 'ClaimBinding') -> 'ClaimBinding'\""
        },
        "retire_input": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord', collection_id: 'int') -> 'bool'\""
        },
        "seal_execution": {
          "kind": "method",
          "signature": "\"(self, claim: 'ClaimBinding', evidence: 'ControllerEvidence', plan: 'WorkflowPlan', target_plan: 'TargetPlan', inputs: 'Iterable[ArtifactSubject]') -> 'None'\""
        },
        "settle_outcomes": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord', evaluation: 'BranchSetEvaluation') -> 'bool'\""
        },
        "target_authority": {
          "kind": "method",
          "signature": "\"(self, claim: 'ClaimBinding', evidence: 'ControllerEvidence', target_plan: 'TargetPlan', inputs: 'Iterable[ArtifactSubject]') -> 'TargetInvocationAuthority'\""
        },
        "verify_and_settle": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord', parent_outcome: 'ParentOutcomeBinding | None' = None) -> 'tuple[OutputCollectionRef, TargetSettlementAuthority | None]'\""
        }
      },
      "signature": "'(*args, **kwargs)'"
    },
    "SchedulerRole": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "SqlAlchemyStateStore": {
      "kind": "class",
      "members": {
        "admit_branch_set": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', decision: 'BranchSetDecision') -> 'WorkRecord'\""
        },
        "admit_join": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', plan: 'JoinPlan', selections: 'Sequence[ArtifactSelection]') -> 'WorkRecord'\""
        },
        "compare_and_swap": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', replacement: 'WorkRecord') -> 'WorkRecord'\""
        },
        "compare_and_swap_cursor": {
          "kind": "method",
          "signature": "\"(self, stream: 'str', *, expected_revision: 'int | None', cursor: 'str') -> 'tuple[str, int]'\""
        },
        "compare_and_swap_evaluation": {
          "kind": "method",
          "signature": "\"(self, evaluation_id: 'str', *, expected_revision: 'int', replacement: 'EvaluationRecord') -> 'EvaluationRecord'\""
        },
        "compare_and_swap_target_production_seal": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetProductionSealRecord') -> 'TargetProductionSealRecord'\""
        },
        "compare_and_swap_target_settlement_seal": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""
        },
        "create": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord') -> 'WorkRecord'\""
        },
        "create_evaluation": {
          "kind": "method",
          "signature": "\"(self, record: 'EvaluationRecord') -> 'EvaluationRecord'\""
        },
        "ensure_target_production_receiving": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord'\""
        },
        "ensure_target_settlement_binding": {
          "kind": "method",
          "signature": "\"(self, record: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""
        },
        "evaluation_store": {
          "kind": "method",
          "signature": "\"(self) -> '_EvaluationStoreView'\""
        },
        "iter_evaluations": {
          "kind": "method",
          "signature": "\"(self, *, phase: 'str | None' = None, query: 'str | None' = None, sort: 'EvaluationSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'Iterator[EvaluationRecord]'\""
        },
        "iter_selection_artifacts": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str') -> 'Iterator[ArtifactSubject]'\""
        },
        "iter_target_dispositions": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[InputDispositionDeclaration]'\""
        },
        "iter_target_outputs": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""
        },
        "iter_target_outputs_by_path": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""
        },
        "iter_target_source_edges": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""
        },
        "iter_target_source_edges_by_input": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""
        },
        "iter_work": {
          "kind": "method",
          "signature": "\"(self, *, phase: 'str | None' = None, query: 'str | None' = None, sort: 'WorkSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'Iterator[WorkRecord]'\""
        },
        "list_evaluations": {
          "kind": "method",
          "signature": "\"(self, *, page_size: 'int' = 25, position: 'tuple[str | int | bool | bytes | None, ...] | None' = None, phase: 'str | None' = None, query: 'str | None' = None, sort: 'EvaluationSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'dict[str, object]'\""
        },
        "list_events": {
          "kind": "method",
          "signature": "\"(self, *, after: 'str | None' = None, limit: 'int' = 100) -> 'Stove0EventPage'\""
        },
        "list_work": {
          "kind": "method",
          "signature": "\"(self, *, page_size: 'int' = 25, position: 'tuple[str | int | bool | bytes | None, ...] | None' = None, phase: 'str | None' = None, query: 'str | None' = None, sort: 'WorkSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'dict[str, object]'\""
        },
        "load": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str') -> 'WorkRecord | None'\""
        },
        "load_cursor": {
          "kind": "method",
          "signature": "\"(self, stream: 'str') -> 'tuple[str, int] | None'\""
        },
        "load_evaluation": {
          "kind": "method",
          "signature": "\"(self, evaluation_id: 'str') -> 'EvaluationRecord | None'\""
        },
        "load_selection": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str') -> 'ArtifactSelection | None'\""
        },
        "load_selection_artifact": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str', artifact_id: 'str') -> 'ArtifactSubject | None'\""
        },
        "load_selection_ref": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str') -> 'ArtifactSelectionRef | None'\""
        },
        "load_target_disposition": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', input_id: 'str') -> 'InputDispositionDeclaration | None'\""
        },
        "load_target_output": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', output_id: 'str') -> 'OutputArtifact | None'\""
        },
        "load_target_production_seal": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord | None'\""
        },
        "load_target_settlement_seal": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetSettlementSealRecord | None'\""
        },
        "prune_operational_state": {
          "kind": "method",
          "signature": "\"(self, *, cutoff: 'str') -> 'dict[str, int]'\""
        },
        "record_target_disposition": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'None'\""
        },
        "record_target_output": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', output: 'OutputArtifact') -> 'None'\""
        },
        "record_target_source_edge": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', edge: 'OutputSourceEdge') -> 'None'\""
        },
        "retain_selection": {
          "kind": "method",
          "signature": "\"(self, selection: 'ArtifactSelection') -> 'None'\""
        },
        "scan_target_production_seals": {
          "kind": "method",
          "signature": "\"(self, *, state: 'TargetProductionSealState', limit: 'int') -> 'tuple[TargetProductionSealRecord, ...]'\""
        },
        "scan_work": {
          "kind": "method",
          "signature": "\"(self, *, phases: 'Sequence[str]', after_work_id: 'str', limit: 'int') -> 'tuple[list[WorkRecord], str]'\""
        },
        "selection_artifact_page": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str', *, continuation: 'str | None', limit: 'int') -> 'tuple[tuple[ArtifactSubject, ...], str | None, bool]'\""
        },
        "target_disposition_page": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str | None', limit: 'int') -> 'tuple[InputDispositionDeclaration, ...]'\""
        },
        "target_output_page": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str | None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""
        },
        "target_output_path_page": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_path: 'str | None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""
        },
        "target_source_edge_page": {
          "kind": "method",
          "signature": "'(self, work_id: \\'str\\', job_id: \\'str\\', *, order: \"Literal[\\'output\\', \\'input\\']\", after_output_id: \\'str | None\\', after_input_id: \\'str | None\\', limit: \\'int\\') -> \\'tuple[OutputSourceEdge, ...]\\''"
        }
      },
      "signature": "\"(database_url: 'str', *, engine: 'Engine | None' = None, initialize: 'bool' = True) -> 'None'\""
    },
    "Stove0Coordinator": {
      "kind": "class",
      "members": {
        "cancel": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str') -> 'WorkRecord'\""
        },
        "create_or_resume": {
          "kind": "method",
          "signature": "\"(self, identity: 'WorkIdentity', *, preview: 'WorkflowPreview | None' = None) -> 'WorkRecord'\""
        },
        "inspect_coordination": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str') -> 'BranchSetEvaluation'\""
        },
        "retry": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str') -> 'WorkRecord'\""
        },
        "step": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str') -> 'WorkRecord'\""
        }
      },
      "signature": "\"(work: 'Stove0WorkService', *, riverhog: 'RiverhogControlPort', planning: 'PlanningPort', observers: 'ObserverPort', targets: 'TargetPort', target_callbacks: 'TargetCallbackPort') -> 'None'\""
    },
    "Stove0RiverhogClient": {
      "kind": "class",
      "members": {
        "abandon_claim": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord') -> 'None'\""
        },
        "abandon_preview_claim": {
          "kind": "method",
          "signature": "\"(self, request: 'WorkflowPreviewRequest', claim: 'ClaimBinding') -> 'None'\""
        },
        "acquire_claim": {
          "kind": "method",
          "signature": "\"(self, work: 'WorkIdentity') -> 'ClaimBinding'\""
        },
        "acquire_preview_claim": {
          "kind": "method",
          "signature": "\"(self, request: 'WorkflowPreviewRequest') -> 'ClaimBinding'\""
        },
        "begin_retirement": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord') -> 'bool'\""
        },
        "observation_authority": {
          "kind": "method",
          "signature": "\"(self, claim: 'ClaimBinding', request: 'ObservationRequest') -> 'ObserverRuntimeAuthority'\""
        },
        "project_target_dispositions": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord', dispositions: 'Sequence[ArtifactDisposition]') -> 'None'\""
        },
        "project_target_source_edges": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord', edges: 'Sequence[ArtifactDispositionOutput]') -> 'None'\""
        },
        "release_claim": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord') -> 'None'\""
        },
        "renew_claim": {
          "kind": "method",
          "signature": "\"(self, work: 'WorkIdentity', claim: 'ClaimBinding') -> 'ClaimBinding'\""
        },
        "restart_claim": {
          "kind": "method",
          "signature": "\"(self, work: 'WorkIdentity', claim: 'ClaimBinding') -> 'ClaimBinding'\""
        },
        "retire_input": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord', collection_id: 'int') -> 'bool'\""
        },
        "seal_execution": {
          "kind": "method",
          "signature": "\"(self, claim: 'ClaimBinding', evidence: 'ControllerEvidence', plan: 'WorkflowPlan', target_plan: 'TargetPlan', inputs: 'Iterable[ArtifactSubject]') -> 'None'\""
        },
        "seal_target_projection": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord') -> 'ArtifactDispositionSetIdentity | None'\""
        },
        "settle_outcomes": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord', evaluation: 'BranchSetEvaluation') -> 'bool'\""
        },
        "target_authority": {
          "kind": "method",
          "signature": "\"(self, claim: 'ClaimBinding', evidence: 'ControllerEvidence', target_plan: 'TargetPlan', inputs: 'Iterable[ArtifactSubject]') -> 'TargetInvocationAuthority'\""
        },
        "verify_and_settle": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord', parent_outcome: 'ParentOutcomeBinding | None' = None) -> 'tuple[OutputCollectionRef, TargetSettlementAuthority | None]'\""
        }
      },
      "signature": "\"(api: 'RiverhogApi', *, claim_lease_seconds: 'int' = 1800, capability_ttl_seconds: 'int' = 900, workspace_assurance: 'WorkspaceAssurance' = 'encrypted', claim_purpose: 'str' = 'stove0-collection-work/v1', state: 'WorkStore | None' = None, authority_batch_size: 'int' = 100) -> 'None'\""
    },
    "Stove0RuntimeConfig": {
      "fields": [
        {
          "default": "required",
          "name": "database_url",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "api_token",
          "type": "'str | None'"
        },
        {
          "default": "required",
          "name": "riverhog_base_url",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "riverhog_token",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "riverhog_allow_insecure_http",
          "type": "'bool'"
        },
        {
          "default": "required",
          "name": "recipes_path",
          "type": "'Path'"
        },
        {
          "default": "required",
          "name": "observers",
          "type": "'dict[str, EndpointRegistration]'"
        },
        {
          "default": "required",
          "name": "targets",
          "type": "'dict[str, EndpointRegistration]'"
        },
        {
          "default": "required",
          "name": "target_callback_base_url",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "target_callback_allow_insecure_http",
          "type": "'bool'"
        },
        {
          "default": "required",
          "name": "target_callback_signing_key",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "target_authority_batch_size",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "workspace_assurance",
          "type": "\"Literal['encrypted', 'ephemeral']\""
        },
        {
          "default": "required",
          "name": "claim_lease_seconds",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "capability_ttl_seconds",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "scheduler_interval_seconds",
          "type": "'float'"
        },
        {
          "default": "required",
          "name": "operational_state_retention_seconds",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "browse_token_signing_key",
          "type": "'str'"
        },
        {
          "default": "AdmissionCatalog(format='stove0-admissions/v1', policies=())",
          "name": "admissions",
          "type": "'AdmissionCatalog'"
        },
        {
          "default": "86400",
          "name": "browse_token_lifetime_seconds",
          "type": "'int'"
        }
      ],
      "kind": "class",
      "members": {
        "from_environment": {
          "kind": "classmethod",
          "signature": "\"(cls, environ: 'Mapping[str, str] | None' = None, *, require_api_token: 'bool' = True) -> 'Stove0RuntimeConfig'\""
        }
      },
      "signature": "'(database_url: \\'str\\', api_token: \\'str | None\\', riverhog_base_url: \\'str\\', riverhog_token: \\'str\\', riverhog_allow_insecure_http: \\'bool\\', recipes_path: \\'Path\\', observers: \\'dict[str, EndpointRegistration]\\', targets: \\'dict[str, EndpointRegistration]\\', target_callback_base_url: \\'str\\', target_callback_allow_insecure_http: \\'bool\\', target_callback_signing_key: \\'str\\', target_authority_batch_size: \\'int\\', workspace_assurance: \"Literal[\\'encrypted\\', \\'ephemeral\\']\", claim_lease_seconds: \\'int\\', capability_ttl_seconds: \\'int\\', scheduler_interval_seconds: \\'float\\', operational_state_retention_seconds: \\'int\\', browse_token_signing_key: \\'str\\', admissions: \\'AdmissionCatalog\\' = AdmissionCatalog(format=\\'stove0-admissions/v1\\', policies=()), browse_token_lifetime_seconds: \\'int\\' = 86400) -> None'"
    },
    "Stove0Scheduler": {
      "kind": "class",
      "members": {
        "advance": {
          "kind": "method",
          "signature": "\"(self, *, role: 'SchedulerRole' = 'combined', limit: 'int' = 25) -> 'dict[str, object]'\""
        },
        "run_once": {
          "kind": "method",
          "signature": "\"(self, *, role: 'SchedulerRole' = 'combined', work_limit: 'int' = 25) -> 'dict[str, object]'\""
        }
      },
      "signature": "\"(*, coordinator: 'Stove0Coordinator', state: 'SqlAlchemyStateStore', production_seals: 'ProductionSealProcessor | None' = None, admission: 'AdmissionProcessor | None' = None, operational_state_retention_seconds: 'int' = 2592000) -> 'None'\""
    },
    "Stove0StateError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "Stove0WorkService": {
      "kind": "class",
      "members": {
        "activate_preplanned": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "activate_preplanned_coordination": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "admit_branch_set": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', decision: 'BranchSetDecision', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "admit_join": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', plan: 'JoinPlan', selections: 'Sequence[ArtifactSelection]', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "begin_observations": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', requests: 'Sequence[ObservationRequest]', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "begin_planning": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "begin_retirement": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', collection_ids: 'Sequence[int]', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "bind_claim": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, claim_id: 'str', fence: 'int', expected_revision: 'int') -> 'WorkRecord'\""
        },
        "bind_target_request": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', request: 'TargetJobRequest', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "cancel": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "complete_abandon": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "create_or_resume": {
          "kind": "method",
          "signature": "\"(self, work: 'WorkIdentity', *, preview: 'WorkflowPreview | None' = None) -> 'WorkRecord'\""
        },
        "fail": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', failure: 'WorkFailure', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "mark_inapplicable": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', outcome: 'WorkInapplicable', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "rebind_claim": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, claim_id: 'str', fence: 'int', expected_revision: 'int') -> 'WorkRecord'\""
        },
        "record_coordination_settlement": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', settlement: 'CoordinationSettlement', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "record_observation": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', result: 'ObservationResult', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "record_retired": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', collection_id: 'int', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "record_target_status": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', status: 'TargetJobStatus', *, operation: 'OperationContract', expected_revision: 'int') -> 'WorkRecord'\""
        },
        "request_coordination_cancel": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "retry_coordination": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, claim_id: 'str', fence: 'int', expected_revision: 'int') -> 'WorkRecord'\""
        },
        "retry_failed": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, claim_id: 'str', fence: 'int', expected_revision: 'int') -> 'WorkRecord'\""
        },
        "seal_target_plan": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, target: 'TargetContract', plan: 'TargetPlan', expected_revision: 'int') -> 'WorkRecord'\""
        },
        "seal_workflow_plan": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', plan: 'WorkflowPlan', *, expected_revision: 'int') -> 'WorkRecord'\""
        },
        "verify_output": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', output: 'OutputCollectionRef', settlement: 'TargetSettlementAuthority', *, expected_revision: 'int') -> 'WorkRecord'\""
        }
      },
      "signature": "\"(store: 'WorkStore') -> 'None'\""
    },
    "TargetCallbackAuthority": {
      "kind": "class",
      "members": {
        "declare_disposition": {
          "kind": "method",
          "signature": "\"(self, token: 'str', *, job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'None'\""
        },
        "declare_output": {
          "kind": "method",
          "signature": "\"(self, token: 'str', *, job_id: 'str', output: 'OutputArtifact') -> 'None'\""
        },
        "declare_source_edge": {
          "kind": "method",
          "signature": "\"(self, token: 'str', *, job_id: 'str', edge: 'OutputSourceEdge') -> 'None'\""
        },
        "input_page": {
          "kind": "method",
          "signature": "\"(self, token: 'str', *, job_id: 'str', continuation: 'str | None', limit: 'int') -> 'TargetInputPage'\""
        },
        "issue_access": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord', target_registration_id: 'str') -> 'TargetCallbackAccess'\""
        },
        "process_due_production_seals": {
          "kind": "method",
          "signature": "\"(self, *, limit: 'int' = 1) -> 'int'\""
        },
        "seal_production": {
          "kind": "method",
          "signature": "\"(self, token: 'str', *, job_id: 'str') -> 'TargetProductionSealResponse'\""
        }
      },
      "signature": "\"(store: 'WorkStore', *, signing_key: 'str', base_url: 'str', allow_insecure_http: 'bool', ttl_seconds: 'int', operations: 'OperationAuthority | None' = None, projector: 'ProductionProjector | None' = None, seal_batch_size: 'int' = 100) -> 'None'\""
    },
    "TargetInvocationAuthority": {
      "fields": [
        {
          "default": "required",
          "name": "runtime",
          "type": "'TargetRuntimeAuthority'"
        },
        {
          "default": "required",
          "name": "workspace_assurance",
          "type": "'WorkspaceAssurance'"
        }
      ],
      "kind": "class",
      "signature": "\"(runtime: 'TargetRuntimeAuthority', workspace_assurance: 'WorkspaceAssurance') -> None\""
    },
    "TargetPort": {
      "kind": "class",
      "members": {
        "cancel_job": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str', request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
        },
        "contract": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str') -> 'TargetContract'\""
        },
        "get_job": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str', request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
        },
        "preflight": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str', request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
        },
        "put_job": {
          "kind": "method",
          "signature": "\"(self, registration_id: 'str', request: 'TargetJobRequest', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
        }
      },
      "signature": "'(*args, **kwargs)'"
    },
    "TerminalPhase": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "WorkFailure": {
      "kind": "class",
      "schema_sha256": "1a48038aecad573b33ff882ab5b826265a854efe9f115ab1e115d1864ec2c4d0",
      "signature": "'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None'"
    },
    "WorkInapplicable": {
      "kind": "class",
      "schema_sha256": "842871cfa20a4e6af7501b4980d0c168945e538de5f72d10eaa49ebb577909cb",
      "signature": "'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None'"
    },
    "WorkPhase": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "WorkRecord": {
      "kind": "class",
      "members": {
        "validate_shape": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        },
        "work_id": {
          "kind": "property",
          "signature": "\"(self) -> 'str'\""
        }
      },
      "schema_sha256": "51fdec684c3f09dbe0e9b0c9a25f47a26c9c87165a72fa7d8b768d3a713d2dc5",
      "signature": "\"(*, format: Literal['stove0-work-record/v1'] = 'stove0-work-record/v1', work: stove0_protocol.models.WorkIdentity, phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'] = 'eligible', revision: Annotated[int, Ge(ge=1)] = 1, claim: stove0_core.work_state.ClaimBinding | None = None, preview_acceptance: stove0_core.work_state.PreviewAcceptance | None = None, expected_target_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observation_requests: tuple[stove0_protocol.models.ObservationRequest, ...] = (), observation_results: tuple[stove0_protocol.models.ObservationResult, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan | None = None, coordination_settlement: stove0_protocol.fork_join.CoordinationSettlement | None = None, join_plan: stove0_protocol.fork_join.JoinPlan | None = None, coordination_cancel_requested: bool = False, workflow_plan: stove0_protocol.models.WorkflowPlan | None = None, target_plan: Optional[Annotated[stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan, FieldInfo(annotation=NoneType, required=True, discriminator='protocol')]] = None, controller_evidence: stove0_protocol.models.ControllerEvidence | None = None, target_request: stove0_target_protocol.protocol.AcceptedTargetJob | None = None, target_status: stove0_target_protocol.protocol.TargetJobStatus | None = None, output: stove0_target_protocol.protocol.OutputCollectionRef | None = None, target_settlement: stove0_target_protocol.protocol.TargetSettlementAuthority | None = None, retirement_remaining: tuple[int, ...] = (), failure: stove0_core.work_state.WorkFailure | None = None, inapplicable: stove0_core.work_state.WorkInapplicable | None = None, abandon_outcome: Optional[Literal['inapplicable', 'failed', 'canceled']] = None) -> None\""
    },
    "WorkStore": {
      "kind": "class",
      "members": {
        "admit_branch_set": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', decision: 'BranchSetDecision') -> 'WorkRecord'\""
        },
        "admit_join": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', plan: 'JoinPlan', selections: 'Sequence[ArtifactSelection]') -> 'WorkRecord'\""
        },
        "compare_and_swap": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', replacement: 'WorkRecord') -> 'WorkRecord'\""
        },
        "compare_and_swap_target_production_seal": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetProductionSealRecord') -> 'TargetProductionSealRecord'\""
        },
        "compare_and_swap_target_settlement_seal": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""
        },
        "create": {
          "kind": "method",
          "signature": "\"(self, record: 'WorkRecord') -> 'WorkRecord'\""
        },
        "ensure_target_production_receiving": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord'\""
        },
        "ensure_target_settlement_binding": {
          "kind": "method",
          "signature": "\"(self, record: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""
        },
        "iter_selection_artifacts": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str') -> 'Iterator[ArtifactSubject]'\""
        },
        "iter_target_dispositions": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[InputDispositionDeclaration]'\""
        },
        "iter_target_outputs": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""
        },
        "iter_target_outputs_by_path": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""
        },
        "iter_target_source_edges": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""
        },
        "iter_target_source_edges_by_input": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputSourceEdge]'\""
        },
        "load": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str') -> 'WorkRecord | None'\""
        },
        "load_selection": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str') -> 'ArtifactSelection | None'\""
        },
        "load_selection_artifact": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str', artifact_id: 'str') -> 'ArtifactSubject | None'\""
        },
        "load_selection_ref": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str') -> 'ArtifactSelectionRef | None'\""
        },
        "load_target_disposition": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', input_id: 'str') -> 'InputDispositionDeclaration | None'\""
        },
        "load_target_output": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', output_id: 'str') -> 'OutputArtifact | None'\""
        },
        "load_target_production_seal": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord | None'\""
        },
        "load_target_settlement_seal": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetSettlementSealRecord | None'\""
        },
        "record_target_disposition": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'None'\""
        },
        "record_target_output": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', output: 'OutputArtifact') -> 'None'\""
        },
        "record_target_source_edge": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', edge: 'OutputSourceEdge') -> 'None'\""
        },
        "retain_selection": {
          "kind": "method",
          "signature": "\"(self, selection: 'ArtifactSelection') -> 'None'\""
        },
        "scan_target_production_seals": {
          "kind": "method",
          "signature": "\"(self, *, state: 'TargetProductionSealState', limit: 'int') -> 'tuple[TargetProductionSealRecord, ...]'\""
        },
        "selection_artifact_page": {
          "kind": "method",
          "signature": "\"(self, selection_sha256: 'str', *, continuation: 'str | None', limit: 'int') -> 'tuple[tuple[ArtifactSubject, ...], str | None, bool]'\""
        },
        "target_disposition_page": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str | None', limit: 'int') -> 'tuple[InputDispositionDeclaration, ...]'\""
        },
        "target_output_page": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str | None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""
        },
        "target_output_path_page": {
          "kind": "method",
          "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_path: 'str | None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""
        },
        "target_source_edge_page": {
          "kind": "method",
          "signature": "'(self, work_id: \\'str\\', job_id: \\'str\\', *, order: \"Literal[\\'output\\', \\'input\\']\", after_output_id: \\'str | None\\', after_input_id: \\'str | None\\', limit: \\'int\\') -> \\'tuple[OutputSourceEdge, ...]\\''"
        }
      },
      "signature": "'(*args, **kwargs)'"
    },
    "WorkflowPreviewService": {
      "kind": "class",
      "members": {
        "preview": {
          "kind": "method",
          "signature": "\"(self, work: 'object') -> 'WorkflowPreview'\""
        }
      },
      "signature": "\"(*, riverhog: 'PreviewRiverhogPort', planning: 'PlanningPort', observers: 'ObserverPort', targets: 'TargetPort') -> 'None'\""
    },
    "database_url_from_environment": {
      "kind": "function",
      "signature": "\"(environ: 'Mapping[str, str] | None' = None) -> 'str'\""
    },
    "project_coordination": {
      "kind": "function",
      "signature": "\"(parent: 'WorkRecord', store: 'WorkStore') -> 'CoordinationProjection'\""
    },
    "scheduler_role": {
      "kind": "function",
      "signature": "\"(value: 'str') -> 'SchedulerRole'\""
    },
    "stove0_state_schema": {
      "kind": "function",
      "signature": "\"(database_url: 'str') -> 'StateSchema'\""
    }
  },
  "module": "stove0_core"
}
```
