# stove0_protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol:fa1fff8d46 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-protocol` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `distribution` | "stove0-protocol" |
| `exports` | additional keys=`ARTIFACT_ID_PATTERN`, `ARTIFACT_SELECTION_FORMAT`, `ARTIFACT_SELECTION_PAGE_MAX`, `ArtifactSelection`, `ArtifactSelectionPage`, `ArtifactSelectionRef`, `ArtifactSubject`, `BRANCH_EFFECT_SETTLEMENT_FORMAT`, `BRANCH_OUTCOME_FORMAT`, `BRANCH_SETTLEMENT_FORMAT`, `BRANCH_SET_FORMAT`, `BranchDeclaration`, `BranchEffectSettlement`, `BranchOutcome`, `BranchOutcomeState`, `BranchPlan`, `BranchSetDecision`, `BranchSetEvaluation`, `BranchSetPlan`, `BranchSettlement`, `BranchTargetPreview`, `BranchWorkBinding`, `CONTROLLER_EVIDENCE_FORMAT`, `COORDINATION_SETTLEMENT_FORMAT`, `CollectionRootRef`, `ControllerEvidence`, `ControllerEvidencePayload`, `CoordinationBranchPlan`, `CoordinationChildSettlementRef`, `CoordinationCollectionResult`, `CoordinationSettlement`, `EVALUATION_DEFINITION_FORMAT`, `EVALUATION_MATRIX_FORMAT`, `EXECUTION_ENVELOPE_FORMAT`, `EvaluationBinding`, `EvaluationDefinition`, `EvaluationDefinitionPayload`, `EvaluationMatrix`, `EvaluationMatrixPayload`, `EvaluationVariant`, `ExecutionEnvelope`, `ExecutionEnvelopePayload`, `ForkJoinBinding`, `JOIN_DECLARATION_FORMAT`, `JOIN_OUTCOME_FORMAT`, `JOIN_PLAN_FORMAT`, `JOIN_SETTLEMENT_FORMAT`, `JSON_SCHEMA_DIALECT`, `JSON_SCHEMA_FORMAT_POLICY`, `JSON_SCHEMA_ONLY_SEMANTIC_PROFILE`, `JSON_SCHEMA_PROFILE_FORMAT`, `JoinDeclaration`, `JoinEvaluationState`, `JoinInputPlan`, `JoinMemberDeclaration`, `JoinOutcome`, `JoinOutcomeState`, `JoinPlan`, `JoinSettlement`, `JoinWorkBinding`, `JoinWorkMemberBinding`, `JsonSchemaDocument`, `OperationRef`, `OperationResultKind`, `PreviewOutcome`, `RIVERHOG_CAPABILITY_TRANSPORT`, `RecipeRef`, `RetirementPolicy`, `RetrievalPolicy`, `SHA256_PATTERN`, `SelectionDocuments`, `SemanticId`, `SemanticValidationProfile`, `SemanticValidationProfilePayload`, `Sha256`, `Stove0ProtocolModel`, `TargetPlanBinding`, `WORKFLOW_PLAN_FORMAT`, `WORKFLOW_PREVIEW_FORMAT`, `WORKFLOW_PREVIEW_REQUEST_FORMAT`, `WORK_FORMAT`, `WorkIdentity`, `WorkPayload`, `WorkflowPlan`, `WorkflowPlanIntent`, `WorkflowPlanPayload`, `WorkflowPreview`, `WorkflowPreviewPayload`, `WorkflowPreviewRequest`, `WorkflowPreviewRequestPayload`, `branch_result_kind`, `branch_work`, `canonical_json_bytes`, `canonical_json_sha256`, `evaluate_branch_set`, `resolve_join_plan`, `resolve_selection`, `update_artifact_selection_commitment`, `validate_branch_set_plan` |
| `module` | "stove0_protocol" |

## Governing policies

- `compatibility/python-api/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:stove0-protocol` — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py::<module>`

### Machine authority

- `/external_contract/python/20`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8f56977cbc68393f5838a9f73039db048286dc4bf36be7cf5c115480c0824f6 -->

```json
{
  "distribution": "stove0-protocol",
  "exports": {
    "ARTIFACT_ID_PATTERN": {
      "kind": "constant",
      "value": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"
    },
    "ARTIFACT_SELECTION_FORMAT": {
      "kind": "constant",
      "value": "stove0-artifact-selection/v1"
    },
    "ARTIFACT_SELECTION_PAGE_MAX": {
      "kind": "constant",
      "value": 256
    },
    "ArtifactSelection": {
      "kind": "class",
      "members": {
        "canonical_artifacts": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'"
        },
        "canonical_bytes": {
          "kind": "method",
          "signature": "(self) -> 'bytes'"
        },
        "ref": {
          "kind": "method",
          "signature": "(self) -> 'ArtifactSelectionRef'"
        },
        "roots": {
          "kind": "method",
          "signature": "(self) -> 'tuple[CollectionRootRef, ...]'"
        },
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, artifacts: 'Sequence[ArtifactSubject]') -> 'ArtifactSelection'"
        },
        "verify_summary_and_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "ddd13318263dfef1cbb2e7911190fc38edd7b4dec41df377a17edcb68cbc328d",
      "signature": "(*, format: Literal['stove0-artifact-selection/v1'] = 'stove0-artifact-selection/v1', artifacts: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)], selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "ArtifactSelectionPage": {
      "kind": "class",
      "members": {
        "bind_page": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "ab841b1329af4b9af6b96b651097d0aadeaf9e86a6033250b4a5a4a5f9d9712c",
      "signature": "(*, authority: stove0_protocol.fork_join.ArtifactSelectionRef, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MaxLen(max_length=256)]) -> None"
    },
    "ArtifactSelectionRef": {
      "kind": "class",
      "members": {
        "from_selection": {
          "kind": "classmethod",
          "signature": "(cls, selection: 'ArtifactSelection') -> 'ArtifactSelectionRef'"
        }
      },
      "schema_sha256": "e8cd0eea202ee0c8f3d19552216aa1b0d50521d431df3a185e911bfe1c3aa4c7",
      "signature": "(*, selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)]) -> None"
    },
    "ArtifactSubject": {
      "kind": "class",
      "members": {
        "canonical_path": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "e232a8cd82feef9c0182803e046cc2d761c6cc9a66de08fcb1aa58eb275a86c6",
      "signature": "(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], collection: stove0_protocol.models.CollectionRootRef, path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None"
    },
    "BRANCH_EFFECT_SETTLEMENT_FORMAT": {
      "kind": "constant",
      "value": "stove0-branch-effect-settlement/v1"
    },
    "BRANCH_OUTCOME_FORMAT": {
      "kind": "constant",
      "value": "stove0-branch-outcome/v1"
    },
    "BRANCH_SETTLEMENT_FORMAT": {
      "kind": "constant",
      "value": "stove0-branch-settlement/v1"
    },
    "BRANCH_SET_FORMAT": {
      "kind": "constant",
      "value": "stove0-branch-set/v1"
    },
    "BranchDeclaration": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "BranchEffectSettlement": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, *, branch: 'BranchPlan', effect_receipt_sha256: 'str') -> 'BranchEffectSettlement'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "8d079076de524eab27cd12f57634171620d2cd76204c4d4349a9231ece5830d8",
      "signature": "(*, format: Literal['stove0-branch-effect-settlement/v1'] = 'stove0-branch-effect-settlement/v1', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effect_receipt_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "BranchOutcome": {
      "kind": "class",
      "members": {
        "exact_declared_plan": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "994be4a7a6a634822af2aeca476070657d99b942dad13078f6d8aa70d683ad3e",
      "signature": "(*, format: Literal['stove0-branch-outcome/v1'] = 'stove0-branch-outcome/v1', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, branch_set_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, state: Literal['failed', 'inapplicable', 'interrupted', 'canceled']) -> None"
    },
    "BranchOutcomeState": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "BranchPlan": {
      "kind": "class",
      "members": {
        "bind_child_work": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        },
        "build": {
          "kind": "classmethod",
          "signature": "(cls, *, parent_work: 'WorkIdentity', branch_id: 'str', decision_sha256: 'str', selection: 'ArtifactSelection', recipe: 'RecipeRef', effective_intent: 'Mapping[str, JsonValue]', workflow_intent: 'WorkflowPlanIntent', observations: 'tuple[ObservationEvidence, ...]' = ()) -> 'BranchPlan'"
        }
      },
      "schema_sha256": "d046c81f10004092168d1b8ac2d4f31967cb97231750a656d0c8cbf085468178",
      "signature": "(*, kind: Literal['leaf'] = 'leaf', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], artifact_selection: stove0_protocol.fork_join.ArtifactSelectionRef, workflow_plan: stove0_protocol.models.WorkflowPlan) -> None"
    },
    "BranchSetDecision": {
      "kind": "class",
      "members": {
        "branch_set_documents": {
          "kind": "property",
          "signature": "(self) -> 'dict[str, BranchSetPlan]'"
        },
        "canonical_branch_sets": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[BranchSetPlan, ...]') -> 'tuple[BranchSetPlan, ...]'"
        },
        "canonical_selections": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[ArtifactSelection, ...]') -> 'tuple[ArtifactSelection, ...]'"
        },
        "complete_documents": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        },
        "leaf_branches": {
          "kind": "method",
          "signature": "(self) -> 'tuple[BranchPlan, ...]'"
        },
        "selection_documents": {
          "kind": "property",
          "signature": "(self) -> 'dict[str, ArtifactSelection]'"
        }
      },
      "schema_sha256": "7002a62c586004f4b0d38994ee4ab49a26cfdf6186d266401bb6e891beca18aa",
      "signature": "(*, plan: stove0_protocol.fork_join.BranchSetPlan, selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...], branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = ()) -> None"
    },
    "BranchSetEvaluation": {
      "kind": "class",
      "schema_sha256": "cf1eb8915ebea499a8253c4e4fca1c5582de70e008456ec3d105232989796362",
      "signature": "(*, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], succeeded_branches: tuple[stove0_protocol.fork_join.BranchSettlement, ...], succeeded_effects: tuple[stove0_protocol.fork_join.BranchEffectSettlement, ...], succeeded_coordinations: tuple[stove0_protocol.fork_join.CoordinationSettlement, ...], unsettled_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], failed_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], inapplicable_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], interrupted_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], canceled_branch_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], join_ready: bool, resolved_join_plan: stove0_protocol.fork_join.JoinPlan | None, join_state: Literal['not-declared', 'waiting', 'ready', 'succeeded', 'failed', 'inapplicable', 'interrupted', 'canceled'], join_settlement: stove0_protocol.fork_join.JoinSettlement | None, unsettled_work_ids: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...], branch_set_succeeded: bool, coordination_settlement: stove0_protocol.fork_join.CoordinationSettlement | None, retirement_requested: bool, coordination_complete_for_retirement: bool) -> None"
    },
    "BranchSetPlan": {
      "kind": "class",
      "members": {
        "canonical_branches": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[BranchDeclaration, ...]') -> 'tuple[BranchDeclaration, ...]'"
        },
        "canonical_bytes": {
          "kind": "method",
          "signature": "(self) -> 'bytes'"
        },
        "canonical_evidence": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'"
        },
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, *, parent_work: 'WorkIdentity', decision_sha256: 'str', evidence_sha256s: 'Sequence[str]' = (), branches: 'Sequence[BranchDeclaration]', join: 'JoinDeclaration | None' = None, retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0, selections: 'SelectionDocuments', branch_sets: 'Mapping[str, BranchSetPlan] | None' = None) -> 'BranchSetPlan'"
        },
        "verify_contract": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "11539570d1e9fdacda5e7aab1672281a8835e6d6b717b0faa07d8da1aabc0294",
      "signature": "(*, format: Literal['stove0-branch-set/v1'] = 'stove0-branch-set/v1', parent_work: stove0_protocol.models.WorkIdentity, decision_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], evidence_sha256s: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...] = (), branches: Annotated[tuple[Annotated[stove0_protocol.fork_join.BranchPlan | stove0_protocol.fork_join.CoordinationBranchPlan, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], join: stove0_protocol.fork_join.JoinDeclaration | None = None, retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "BranchSettlement": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, *, branch: 'BranchPlan', derivation_sha256: 'str', producer_settlement_sha256: 'str', output_collection: 'CollectionRootRef', output_selection: 'ArtifactSelection') -> 'BranchSettlement'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "bfc41425873e5fbc835023b004e10a0a7759c5726572d11f26def01105144ab1",
      "signature": "(*, format: Literal['stove0-branch-settlement/v1'] = 'stove0-branch-settlement/v1', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "BranchTargetPreview": {
      "kind": "class",
      "schema_sha256": "c4d10f0d0aedba13309965e3328e4ebf83e86cab353244860f70f7d01e3d2647",
      "signature": "(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_plan: stove0_protocol.models.TargetPlanBinding) -> None"
    },
    "BranchWorkBinding": {
      "kind": "class",
      "schema_sha256": "0c3297729fbc53bc25d0dcb1bf587a91b7991848f5584c8248180f28b506ecd9",
      "signature": "(*, kind: Literal['branch'] = 'branch', parent_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], decision_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], artifact_selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "CONTROLLER_EVIDENCE_FORMAT": {
      "kind": "constant",
      "value": "stove0-controller-evidence/v1"
    },
    "COORDINATION_SETTLEMENT_FORMAT": {
      "kind": "constant",
      "value": "stove0-coordination-settlement/v1"
    },
    "CollectionRootRef": {
      "kind": "class",
      "members": {
        "from_identity": {
          "kind": "classmethod",
          "signature": "(cls, value: 'CollectionRootIdentity') -> 'CollectionRootRef'"
        },
        "to_identity": {
          "kind": "method",
          "signature": "(self) -> 'CollectionRootIdentity'"
        }
      },
      "schema_sha256": "0129b21c7d9e83eb25d287865778230c2388dd72c6a5fa8243a72b6c74a72649",
      "signature": "(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "ControllerEvidence": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'ControllerEvidencePayload') -> 'ControllerEvidence'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "4083962837a0d61423ed20edd5491176f060c2344623a14012265212866cd177",
      "signature": "(*, format: Literal['stove0-controller-evidence/v1'] = 'stove0-controller-evidence/v1', execution_envelope: stove0_protocol.models.ExecutionEnvelope, controller_evidence_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "ControllerEvidencePayload": {
      "kind": "class",
      "schema_sha256": "3c3aad377f9e1e0b63e741284adbda66871bea711a3f40763d750f55966ed91b",
      "signature": "(*, format: Literal['stove0-controller-evidence/v1'] = 'stove0-controller-evidence/v1', execution_envelope: stove0_protocol.models.ExecutionEnvelope) -> None"
    },
    "CoordinationBranchPlan": {
      "kind": "class",
      "members": {
        "bind_child_work": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        },
        "build": {
          "kind": "classmethod",
          "signature": "(cls, *, parent_work: 'WorkIdentity', branch_id: 'str', decision_sha256: 'str', selection: 'ArtifactSelection', recipe: 'RecipeRef', effective_intent: 'Mapping[str, JsonValue]', branch_set_sha256: 'str') -> 'CoordinationBranchPlan'"
        },
        "build_work": {
          "kind": "classmethod",
          "signature": "(cls, *, parent_work: 'WorkIdentity', branch_id: 'str', decision_sha256: 'str', selection: 'ArtifactSelection', recipe: 'RecipeRef', effective_intent: 'Mapping[str, JsonValue]') -> 'WorkIdentity'"
        }
      },
      "schema_sha256": "fbeeb2288a21de92836dad26dc3ecea68cdd3d8cb85863660d91fc66420ea16d",
      "signature": "(*, kind: Literal['coordination'] = 'coordination', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], artifact_selection: stove0_protocol.fork_join.ArtifactSelectionRef, work: stove0_protocol.models.WorkIdentity, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "CoordinationChildSettlementRef": {
      "kind": "class",
      "schema_sha256": "7bd31023cc09897af3321f85d7438ce508e1093e2abf51326d29362ab02bb869",
      "signature": "(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], kind: Literal['collection', 'external-effect', 'coordination'], settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "CoordinationCollectionResult": {
      "kind": "class",
      "schema_sha256": "5fd6c013e6da69ffca712f53a43fde736783250dd5dda7a0a176a2a9f4eeb5bf",
      "signature": "(*, producer_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef) -> None"
    },
    "CoordinationSettlement": {
      "kind": "class",
      "members": {
        "canonical_children": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[CoordinationChildSettlementRef, ...]') -> 'tuple[CoordinationChildSettlementRef, ...]'"
        },
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, *, plan: 'BranchSetPlan', collection_settlements: 'Sequence[BranchSettlement]', effect_settlements: 'Sequence[BranchEffectSettlement]', coordination_settlements: 'Sequence[CoordinationSettlement]', join_settlement: 'JoinSettlement | None') -> 'CoordinationSettlement'"
        },
        "verify_contract": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "3dd77a54fcedc8cb14f2c4883a0396b657d5c6c84bb02b421866471570ac44b7",
      "signature": "(*, format: Literal['stove0-coordination-settlement/v1'] = 'stove0-coordination-settlement/v1', work: stove0_protocol.models.WorkIdentity, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], children: tuple[stove0_protocol.fork_join.CoordinationChildSettlementRef, ...], contains_external_effects: bool, final_join_settlement_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, collection_result: stove0_protocol.fork_join.CoordinationCollectionResult | None = None, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "EVALUATION_DEFINITION_FORMAT": {
      "kind": "constant",
      "value": "stove0-evaluation-definition/v1"
    },
    "EVALUATION_MATRIX_FORMAT": {
      "kind": "constant",
      "value": "stove0-evaluation-matrix/v1"
    },
    "EXECUTION_ENVELOPE_FORMAT": {
      "kind": "constant",
      "value": "stove0-execution-envelope/v1"
    },
    "EvaluationBinding": {
      "kind": "class",
      "schema_sha256": "88e9c364a4a75880b79baef14a38f00d4aed59886831937d8bad72f471a5b04b",
      "signature": "(*, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], matrix_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], variant_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], parameters: dict[str, JsonValue] = <factory>) -> None"
    },
    "EvaluationDefinition": {
      "kind": "class",
      "members": {
        "child_work": {
          "kind": "method",
          "signature": "(self, variant_id: 'str') -> 'WorkIdentity'"
        },
        "child_works": {
          "kind": "method",
          "signature": "(self) -> 'tuple[WorkIdentity, ...]'"
        },
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'EvaluationDefinitionPayload') -> 'EvaluationDefinition'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "73dde42300b8d2864bca552e753011ebc0e62eac66f6181892291fd37f0e7977",
      "signature": "(*, format: Literal['stove0-evaluation-definition/v1'] = 'stove0-evaluation-definition/v1', purpose: Literal['trial', 'evaluation'] = 'evaluation', recipe: stove0_protocol.models.RecipeRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], common_intent: dict[str, JsonValue] = <factory>, matrix: stove0_protocol.models.EvaluationMatrix, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "EvaluationDefinitionPayload": {
      "kind": "class",
      "members": {
        "canonical_inputs": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[CollectionRootRef, ...]') -> 'tuple[CollectionRootRef, ...]'"
        },
        "validate_purpose": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "c07e0127adc95a58e9faaa49fce7798075248e5b13b49b762ae9f37be3d4f62a",
      "signature": "(*, format: Literal['stove0-evaluation-definition/v1'] = 'stove0-evaluation-definition/v1', purpose: Literal['trial', 'evaluation'] = 'evaluation', recipe: stove0_protocol.models.RecipeRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], common_intent: dict[str, JsonValue] = <factory>, matrix: stove0_protocol.models.EvaluationMatrix) -> None"
    },
    "EvaluationMatrix": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'EvaluationMatrixPayload') -> 'EvaluationMatrix'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "2d0bb04bdfeb8d3e6f9fab1b20a84df01677032f669043d7d567d690230c8960",
      "signature": "(*, format: Literal['stove0-evaluation-matrix/v1'] = 'stove0-evaluation-matrix/v1', variants: Annotated[tuple[stove0_protocol.models.EvaluationVariant, ...], MinLen(min_length=1)], matrix_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "EvaluationMatrixPayload": {
      "kind": "class",
      "members": {
        "canonical_variants": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[EvaluationVariant, ...]') -> 'tuple[EvaluationVariant, ...]'"
        }
      },
      "schema_sha256": "f729430d8ae4e5c0ba3f436617b5f23f042b6dc238ae5f68a8b5b035ff853614",
      "signature": "(*, format: Literal['stove0-evaluation-matrix/v1'] = 'stove0-evaluation-matrix/v1', variants: Annotated[tuple[stove0_protocol.models.EvaluationVariant, ...], MinLen(min_length=1)]) -> None"
    },
    "EvaluationVariant": {
      "kind": "class",
      "schema_sha256": "5cec278805e11bb3d57749f49b73debbcf8efbe5de4cb285d198578acb72bab1",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], parameters: dict[str, JsonValue] = <factory>) -> None"
    },
    "ExecutionEnvelope": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'ExecutionEnvelopePayload') -> 'ExecutionEnvelope'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "5f6955d3291863e0bdb8bc5e043504aa895c10bca411253c174cf22da6af370f",
      "signature": "(*, format: Literal['stove0-execution-envelope/v1'] = 'stove0-execution-envelope/v1', claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], workflow_plan: stove0_protocol.models.WorkflowPlan, target_plan: stove0_protocol.models.TargetPlanBinding, execution_envelope_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "ExecutionEnvelopePayload": {
      "kind": "class",
      "members": {
        "bind_target": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        },
        "canonical_claim_id": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "07fd0c9dee16b72ccd9cb2e451adfdd8515d19c459761a5e09fc57daea06cf92",
      "signature": "(*, format: Literal['stove0-execution-envelope/v1'] = 'stove0-execution-envelope/v1', claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], workflow_plan: stove0_protocol.models.WorkflowPlan, target_plan: stove0_protocol.models.TargetPlanBinding) -> None"
    },
    "ForkJoinBinding": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "JOIN_DECLARATION_FORMAT": {
      "kind": "constant",
      "value": "stove0-join-declaration/v1"
    },
    "JOIN_OUTCOME_FORMAT": {
      "kind": "constant",
      "value": "stove0-join-outcome/v1"
    },
    "JOIN_PLAN_FORMAT": {
      "kind": "constant",
      "value": "stove0-join-plan/v1"
    },
    "JOIN_SETTLEMENT_FORMAT": {
      "kind": "constant",
      "value": "stove0-join-settlement/v1"
    },
    "JSON_SCHEMA_DIALECT": {
      "kind": "constant",
      "value": "https://json-schema.org/draft/2020-12/schema"
    },
    "JSON_SCHEMA_FORMAT_POLICY": {
      "kind": "constant",
      "value": "annotation-only"
    },
    "JSON_SCHEMA_ONLY_SEMANTIC_PROFILE": {
      "kind": "object",
      "type": "stove0_protocol.models.SemanticValidationProfile"
    },
    "JSON_SCHEMA_PROFILE_FORMAT": {
      "kind": "constant",
      "value": "stove0-json-schema-profile/v1"
    },
    "JoinDeclaration": {
      "kind": "class",
      "members": {
        "canonical_members": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[JoinMemberDeclaration, ...]') -> 'tuple[JoinMemberDeclaration, ...]'"
        },
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, *, members: 'Sequence[JoinMemberDeclaration]', recipe: 'RecipeRef', effective_intent: 'Mapping[str, JsonValue]', workflow_intent: 'WorkflowPlanIntent') -> 'JoinDeclaration'"
        },
        "verify_contract": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "59b7a82cb0367a896054d30d8696a7585892d491b7e53c80ef5205434d902f06",
      "signature": "(*, format: Literal['stove0-join-declaration/v1'] = 'stove0-join-declaration/v1', members: Annotated[tuple[stove0_protocol.fork_join.JoinMemberDeclaration, ...], MinLen(min_length=2)], recipe: stove0_protocol.models.RecipeRef, effective_intent: dict[str, JsonValue] = <factory>, workflow_intent: stove0_protocol.models.WorkflowPlanIntent, join_declaration_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "JoinEvaluationState": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "JoinInputPlan": {
      "kind": "class",
      "schema_sha256": "f72983c19a5bfa012c6232998bbdeb509fb48d55104fd73099901edd97e16c18",
      "signature": "(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, artifact_selection: stove0_protocol.fork_join.ArtifactSelectionRef) -> None"
    },
    "JoinMemberDeclaration": {
      "kind": "class",
      "members": {
        "canonical_roles": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'"
        }
      },
      "schema_sha256": "f1b1441297c6334a7eeb79e0a05030fe3b536cc2d6ee09a2aa316a623e74c389",
      "signature": "(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], output_roles: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)]) -> None"
    },
    "JoinOutcome": {
      "kind": "class",
      "schema_sha256": "8245153ec249680ee6fa42b10141c4f16e28f89bb7863c1baf10dbdcd336d1fe",
      "signature": "(*, format: Literal['stove0-join-outcome/v1'] = 'stove0-join-outcome/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['failed', 'inapplicable', 'interrupted', 'canceled']) -> None"
    },
    "JoinOutcomeState": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "JoinPlan": {
      "kind": "class",
      "members": {
        "canonical_inputs": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[JoinInputPlan, ...]') -> 'tuple[JoinInputPlan, ...]'"
        },
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, *, parent_work_id: 'str', branch_set_sha256: 'str', declaration: 'JoinDeclaration', inputs: 'Sequence[JoinInputPlan]', work: 'WorkIdentity', workflow_plan: 'WorkflowPlan') -> 'JoinPlan'"
        },
        "verify_contract": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "8210d3195621df847cfbceceea08fa633e6b88328b7bba90ca04fc6faa47280a",
      "signature": "(*, format: Literal['stove0-join-plan/v1'] = 'stove0-join-plan/v1', parent_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], declaration: stove0_protocol.fork_join.JoinDeclaration, inputs: Annotated[tuple[stove0_protocol.fork_join.JoinInputPlan, ...], MinLen(min_length=2)], work: stove0_protocol.models.WorkIdentity, workflow_plan: stove0_protocol.models.WorkflowPlan, join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "JoinSettlement": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, *, plan: 'JoinPlan', derivation_sha256: 'str', producer_settlement_sha256: 'str', output_collection: 'CollectionRootRef', output_selection: 'ArtifactSelection') -> 'JoinSettlement'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "83bf96fc750c1f70c2d908703e2f4628e73de4371e64882d7f8421d19869db32",
      "signature": "(*, format: Literal['stove0-join-settlement/v1'] = 'stove0-join-settlement/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "JoinWorkBinding": {
      "kind": "class",
      "members": {
        "canonical_members": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[JoinWorkMemberBinding, ...]') -> 'tuple[JoinWorkMemberBinding, ...]'"
        }
      },
      "schema_sha256": "9466a9060b07855b7c74730ec2a1866e96a403f35ff43ce1352e6a4eb80eeafd",
      "signature": "(*, kind: Literal['join'] = 'join', parent_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], members: Annotated[tuple[stove0_protocol.models.JoinWorkMemberBinding, ...], MinLen(min_length=2)]) -> None"
    },
    "JoinWorkMemberBinding": {
      "kind": "class",
      "schema_sha256": "e690f8cf01c0d8d6a830f122677b627aae3106fead58d4c6f8829b28574369e3",
      "signature": "(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, artifact_selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "JsonSchemaDocument": {
      "kind": "class",
      "members": {
        "from_schema": {
          "kind": "classmethod",
          "signature": "(cls, schema_id: 'str', schema: 'dict[str, JsonValue]') -> 'JsonSchemaDocument'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "044aac56d1ab78dca2a949caf25694292222a652e8b22484284cfd16c793e05f",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], dialect: Literal['https://json-schema.org/draft/2020-12/schema'] = 'https://json-schema.org/draft/2020-12/schema', format_policy: Literal['annotation-only'] = 'annotation-only', schema: dict[str, JsonValue]) -> None"
    },
    "OperationRef": {
      "kind": "class",
      "members": {
        "from_identity": {
          "kind": "classmethod",
          "signature": "(cls, value: 'OperationIdentity') -> 'OperationRef'"
        },
        "to_identity": {
          "kind": "method",
          "signature": "(self) -> 'OperationIdentity'"
        }
      },
      "schema_sha256": "b0a71e15638b56b26ba692329ca73014bfa5bf6026623cec421455689c8542f8",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "OperationResultKind": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "PreviewOutcome": {
      "kind": "class",
      "schema_sha256": "83bbb579a08b1ff84c5c4771b7f20a911c71ba952de33cb6bc91c9db92b4f632",
      "signature": "(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool | None = None) -> None"
    },
    "RIVERHOG_CAPABILITY_TRANSPORT": {
      "kind": "constant",
      "value": "riverhog-capability/v1"
    },
    "RecipeRef": {
      "kind": "class",
      "members": {
        "from_identity": {
          "kind": "classmethod",
          "signature": "(cls, value: 'RecipeIdentity') -> 'RecipeRef'"
        },
        "to_identity": {
          "kind": "method",
          "signature": "(self) -> 'RecipeIdentity'"
        }
      },
      "schema_sha256": "3790d8f811ebb13fed76cbd0a115a818961abe76707214c58706e9033077b650",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[int, Ge(ge=1)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "RetirementPolicy": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "RetrievalPolicy": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "SHA256_PATTERN": {
      "kind": "constant",
      "value": "^[0-9a-f]{64}$"
    },
    "SelectionDocuments": {
      "kind": "object",
      "type": "types.GenericAlias"
    },
    "SemanticId": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "SemanticValidationProfile": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'SemanticValidationProfilePayload') -> 'SemanticValidationProfile'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "6b8688e96bcf912f0eab842d27ff10cea7bc3bb1b53f4b5510968ba90d9d1c9b",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], rules: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)], conformance_vectors_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, profile_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "SemanticValidationProfilePayload": {
      "kind": "class",
      "members": {
        "canonical_rules": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'"
        }
      },
      "schema_sha256": "9feba86c6b0478a980740aa7d15a0534fd499479fc164ec3f3b3f3b72b5182f9",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], rules: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)], conformance_vectors_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None) -> None"
    },
    "Sha256": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "Stove0ProtocolModel": {
      "kind": "class",
      "schema_sha256": "eab4af485f9e83d7d3b54228c0c1741c3ebd306c2b4facb16c07d837e2381601",
      "signature": "() -> None"
    },
    "TargetPlanBinding": {
      "kind": "class",
      "members": {
        "require_plan_document": {
          "kind": "classmethod",
          "signature": "(cls, value: 'dict[str, JsonValue]') -> 'dict[str, JsonValue]'"
        }
      },
      "schema_sha256": "7682f9c07f20e2427e0662961cc33879f27f42dfe6ab6aa84eba726de0c6803e",
      "signature": "(*, protocol: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan: dict[str, JsonValue], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "WORKFLOW_PLAN_FORMAT": {
      "kind": "constant",
      "value": "stove0-workflow-plan/v1"
    },
    "WORKFLOW_PREVIEW_FORMAT": {
      "kind": "constant",
      "value": "stove0-workflow-preview/v1"
    },
    "WORKFLOW_PREVIEW_REQUEST_FORMAT": {
      "kind": "constant",
      "value": "stove0-workflow-preview-request/v1"
    },
    "WORK_FORMAT": {
      "kind": "constant",
      "value": "stove0-work/v1"
    },
    "WorkIdentity": {
      "kind": "class",
      "members": {
        "root_identities": {
          "kind": "method",
          "signature": "(self) -> 'tuple[CollectionRootIdentity, ...]'"
        },
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'WorkPayload') -> 'WorkIdentity'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "23995cc54c7aa6ac8e45d8e3eeaedcd6d3ecf87ea8a383e5ffa294f37b01e0c4",
      "signature": "(*, format: Literal['stove0-work/v1'] = 'stove0-work/v1', recipe: stove0_protocol.models.RecipeRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, evaluation: stove0_protocol.models.EvaluationBinding | None = None, fork_join: Optional[Annotated[stove0_protocol.models.BranchWorkBinding | stove0_protocol.models.JoinWorkBinding, FieldInfo(annotation=NoneType, required=True, discriminator='kind')]] = None, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "WorkPayload": {
      "kind": "class",
      "members": {
        "canonical_inputs": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[CollectionRootRef, ...]') -> 'tuple[CollectionRootRef, ...]'"
        }
      },
      "schema_sha256": "c57f688649123bb4d7957af123bc8dce24ad0ff95884532522d0076ca01e702f",
      "signature": "(*, format: Literal['stove0-work/v1'] = 'stove0-work/v1', recipe: stove0_protocol.models.RecipeRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, evaluation: stove0_protocol.models.EvaluationBinding | None = None, fork_join: Optional[Annotated[stove0_protocol.models.BranchWorkBinding | stove0_protocol.models.JoinWorkBinding, FieldInfo(annotation=NoneType, required=True, discriminator='kind')]] = None) -> None"
    },
    "WorkflowPlan": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'WorkflowPlanPayload') -> 'WorkflowPlan'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "dc8f2cc9d162e6b40e2d3abe594494d91c2fa0d071396788b9a0b7df86f64088",
      "signature": "(*, format: Literal['stove0-workflow-plan/v1'] = 'stove0-workflow-plan/v1', work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), operation: stove0_protocol.models.OperationRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>, workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "WorkflowPlanIntent": {
      "kind": "class",
      "members": {
        "from_plan": {
          "kind": "classmethod",
          "signature": "(cls, plan: 'WorkflowPlan') -> 'WorkflowPlanIntent'"
        },
        "materialize": {
          "kind": "method",
          "signature": "(self, *, work: 'WorkIdentity', observations: 'tuple[ObservationEvidence, ...]' = ()) -> 'WorkflowPlan'"
        },
        "validate_retirement": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "3ee63b3d0a336d7ca1790729a7ffab36688bc08ad7cc692e025e59701be915b8",
      "signature": "(*, operation: stove0_protocol.models.OperationRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>) -> None"
    },
    "WorkflowPlanPayload": {
      "kind": "class",
      "members": {
        "canonical_observations": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'"
        },
        "protect_evaluation_sources": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "e13c1c2b2adac8dde0ce9197a2b19bded89f69aa6ec359fd68445674beb8af60",
      "signature": "(*, format: Literal['stove0-workflow-plan/v1'] = 'stove0-workflow-plan/v1', work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), operation: stove0_protocol.models.OperationRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>) -> None"
    },
    "WorkflowPreview": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'WorkflowPreviewPayload') -> 'WorkflowPreview'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "6151382a075394584ab8c2e21fde7b1cf1955791b21e983ab5119792ed3f02de",
      "signature": "(*, format: Literal['stove0-workflow-preview/v1'] = 'stove0-workflow-preview/v1', preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['ready', 'inapplicable', 'failed', 'canceled'], work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan | None = None, branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = (), selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...] = (), target_plans: tuple[stove0_protocol.fork_join.BranchTargetPreview, ...] = (), outcome: stove0_protocol.models.PreviewOutcome | None = None, warnings: tuple[str, ...] = (), preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "WorkflowPreviewPayload": {
      "kind": "class",
      "members": {
        "canonical_child_branch_sets": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[BranchSetPlan, ...]') -> 'tuple[BranchSetPlan, ...]'"
        },
        "canonical_observations": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'"
        },
        "canonical_selections": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[ArtifactSelection, ...]') -> 'tuple[ArtifactSelection, ...]'"
        },
        "canonical_target_plans": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[BranchTargetPreview, ...]') -> 'tuple[BranchTargetPreview, ...]'"
        },
        "canonical_warnings": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'"
        },
        "validate_state": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "d00182a18c04ab8c9d3c43ca2ae7af712130f80e1cb52991252abfadbddcb4a9",
      "signature": "(*, format: Literal['stove0-workflow-preview/v1'] = 'stove0-workflow-preview/v1', preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['ready', 'inapplicable', 'failed', 'canceled'], work: stove0_protocol.models.WorkIdentity, observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan | None = None, branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = (), selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...] = (), target_plans: tuple[stove0_protocol.fork_join.BranchTargetPreview, ...] = (), outcome: stove0_protocol.models.PreviewOutcome | None = None, warnings: tuple[str, ...] = ()) -> None"
    },
    "WorkflowPreviewRequest": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'WorkflowPreviewRequestPayload') -> 'WorkflowPreviewRequest'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "afc33b01c4464932d1c696a5d1d691bfc9464b94ff42960587ae8cfcdb140be9",
      "signature": "(*, format: Literal['stove0-workflow-preview-request/v1'] = 'stove0-workflow-preview-request/v1', work: stove0_protocol.models.WorkIdentity, preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "WorkflowPreviewRequestPayload": {
      "kind": "class",
      "schema_sha256": "48a667fcabde7106f7af21385c2e4c9c5f1a4d2285a7c5171d6a4b9840078ae5",
      "signature": "(*, format: Literal['stove0-workflow-preview-request/v1'] = 'stove0-workflow-preview-request/v1', work: stove0_protocol.models.WorkIdentity) -> None"
    },
    "branch_result_kind": {
      "kind": "function",
      "signature": "(branch: 'BranchDeclaration', branch_sets: 'Mapping[str, BranchSetPlan]') -> \"Literal['collection', 'external-effect', 'coordination']\""
    },
    "branch_work": {
      "kind": "function",
      "signature": "(branch: 'BranchDeclaration') -> 'WorkIdentity'"
    },
    "canonical_json_bytes": {
      "kind": "function",
      "signature": "(value: 'object') -> 'bytes'"
    },
    "canonical_json_sha256": {
      "kind": "function",
      "signature": "(value: 'object') -> 'str'"
    },
    "evaluate_branch_set": {
      "kind": "function",
      "signature": "(plan: 'BranchSetPlan', selections: 'SelectionDocuments', *, branch_sets: 'Mapping[str, BranchSetPlan] | None' = None, branch_settlements: 'Sequence[BranchSettlement]' = (), branch_effect_settlements: 'Sequence[BranchEffectSettlement]' = (), branch_coordination_settlements: 'Sequence[CoordinationSettlement]' = (), branch_outcomes: 'Sequence[BranchOutcome]' = (), join_settlement: 'JoinSettlement | None' = None, join_outcome: 'JoinOutcome | None' = None) -> 'BranchSetEvaluation'"
    },
    "resolve_join_plan": {
      "kind": "function",
      "signature": "(plan: 'BranchSetPlan', selections: 'SelectionDocuments', settlements: 'Sequence[BranchSettlement]', effect_settlements: 'Sequence[BranchEffectSettlement]' = (), coordination_settlements: 'Sequence[CoordinationSettlement]' = (), branch_sets: 'Mapping[str, BranchSetPlan] | None' = None) -> 'tuple[JoinPlan, tuple[ArtifactSelection, ...]] | None'"
    },
    "resolve_selection": {
      "kind": "function",
      "signature": "(reference: 'ArtifactSelectionRef', selections: 'SelectionDocuments') -> 'ArtifactSelection'"
    },
    "update_artifact_selection_commitment": {
      "kind": "function",
      "signature": "(digest: 'Any', *, ordinal: 'int', artifact: 'ArtifactSubject') -> 'None'"
    },
    "validate_branch_set_plan": {
      "kind": "function",
      "signature": "(plan: 'BranchSetPlan', selections: 'SelectionDocuments', branch_sets: 'Mapping[str, BranchSetPlan] | None' = None) -> 'None'"
    }
  },
  "module": "stove0_protocol"
}
```
