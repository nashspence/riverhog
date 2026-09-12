# stove0_operator_contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts:22b04821ad -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-51f3ead2ada9) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9a2627e76f32"></a>
| Field | Shape |
|---|---|
| <a id="s-1ffcdaaff8fa"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-949fc2301452"></a>`exports` | additional keys=`ADMISSION_POLICY_COUNT_MAX`, `AdmissionCatalog`, `AdmissionIntent`, `AdmissionPage`, `AdmissionPhase`, `AdmissionPolicy`, `AdmissionPolicyCatalogView`, `AdmissionPolicyStatus`, `AdmissionRun`, `AdmissionSort`, `AdmissionState`, `AdmissionView`, `ArtifactSelectionPage`, `BRANCH_SET_ADMITTED`, `BranchSetAdmittedEvent`, `BranchSetAdmittedEventData`, `EVALUATION_CREATED`, `EVALUATION_UPDATED`, `EvaluationChildView`, `EvaluationCreatedEvent`, `EvaluationCreatedEventData`, `EvaluationPage`, `EvaluationPhase`, `EvaluationReviewIn`, `EvaluationReviewView`, `EvaluationSort`, `EvaluationUpdatedEvent`, `EvaluationUpdatedEventData`, `EvaluationView`, `JOIN_ADMITTED`, `JoinAdmittedEvent`, `JoinAdmittedEventData`, `RecipeCatalogView`, `RecipeView`, `STOVE0_EVENT_SOURCE`, `STOVE0_EVENT_TYPES`, `STOVE0_HTTP_ERROR_AUTHORITY`, `SchedulerFailure`, `SchedulerPruning`, `SchedulerRole`, `SchedulerRun`, `SchedulerRunIn`, `SchedulerStatus`, `SchedulerWorkBatch`, `SortOrder`, `Stove0CloudEvent`, `Stove0EventData`, `Stove0EventPage`, `Stove0EventType`, `Stove0LifecycleEvent`, `WORK_CREATED`, `WORK_UPDATED`, `WorkClaimView`, `WorkCreateIn`, `WorkCreatedEvent`, `WorkCreatedEventData`, `WorkFailureView`, `WorkInapplicableView`, `WorkPage`, `WorkPhase`, `WorkSort`, `WorkUpdatedEvent`, `WorkUpdatedEventData`, `WorkView`, `WorkflowPreviewIn`, `parse_stove0_event`, `stove0_event`, `validate_evaluation_child_shape`, `validate_evaluation_review_shape`, `validate_evaluation_state_shape`, `validate_work_state_shape` |
| <a id="s-0d61db01d2af"></a>`module` | "stove0_operator_contracts" |

## Governing policies

- <a id="pa-23165251a33b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba506)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts](../../../evidence/sources.md#src-8f69eee57b84) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py::<module>`

### Machine authority

- `/external_contract/python/19`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3df1738b905e5c3871393392864f097c9cb37c799a6a8a82d96e828529c0c54f -->

```json
{
  "distribution": "stove0-operator-contracts",
  "exports": {
    "ADMISSION_POLICY_COUNT_MAX": {
      "kind": "constant",
      "value": 100
    },
    "AdmissionCatalog": {
      "kind": "class",
      "members": {
        "canonical_policies": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[AdmissionPolicy, ...]') -> 'tuple[AdmissionPolicy, ...]'"
        },
        "catalog_sha256": {
          "kind": "property",
          "signature": "(self) -> 'str'"
        }
      },
      "schema_sha256": "00ecac8a3c71b27fc2027d9df838959cdfe480be931fd7518ea0f785f84b01c0",
      "signature": "(*, format: Literal['stove0-admissions/v1'] = 'stove0-admissions/v1', policies: Annotated[tuple[stove0_operator_contracts.AdmissionPolicy, ...], MaxLen(max_length=100)] = ()) -> None"
    },
    "AdmissionIntent": {
      "kind": "class",
      "members": {
        "exact_identity": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        },
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, *, policy: 'AdmissionPolicy', collection: 'CatalogSyncDescriptor') -> 'AdmissionIntent'"
        }
      },
      "schema_sha256": "7a441cd9649c0ef9f6bb98d6e3d7ad782e0452a42d48382efff22f61ca6a3078",
      "signature": "(*, format: Literal['stove0-admission-intent/v1'] = 'stove0-admission-intent/v1', admission_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], policy_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], policy_revision: Annotated[int, Ge(ge=1)], policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], required_tags: tuple[CollectionTag, ...], collection: riverhog_protocol.catalog_sync.CatalogSyncDescriptor, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int, Ge(ge=1)], recipe_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effective_intent: dict[str, JsonValue]) -> None"
    },
    "AdmissionPage": {
      "kind": "class",
      "schema_sha256": "1c999fdfd073ab6da2b05a6189166ce2f7dbff29a64c6e8e32695cff71e09607",
      "signature": "(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, sort: Literal['created_at', 'updated_at', 'state', 'admission_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], policy_id: str | None, state: Optional[Literal['intent', 'previewed', 'work_bound']], admissions: tuple[stove0_operator_contracts.AdmissionView, ...]) -> None"
    },
    "AdmissionPhase": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "AdmissionPolicy": {
      "kind": "class",
      "members": {
        "canonical_required_tags": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[CollectionTag, ...]') -> 'tuple[CollectionTag, ...]'"
        },
        "policy_sha256": {
          "kind": "property",
          "signature": "(self) -> 'str'"
        }
      },
      "schema_sha256": "5751b77c2fcfd5f1f89e28c406af3c3c97713360ffb17dbcfaa02a2c2f414907",
      "signature": "(*, format: Literal['stove0-admission-policy/v1'] = 'stove0-admission-policy/v1', id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], revision: Annotated[int, Ge(ge=1)], required_tags: Annotated[tuple[CollectionTag, ...], MinLen(min_length=1), MaxLen(max_length=100)], recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int, Ge(ge=1)], recipe_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effective_intent: dict[str, JsonValue] = <factory>, automatic_preview: Literal['accept-ready'] = 'accept-ready') -> None"
    },
    "AdmissionPolicyCatalogView": {
      "kind": "class",
      "schema_sha256": "ec3d8568a5b10756746eae442f63e658e3f9aff83a690be9759d4d7ef30748d3",
      "signature": "(*, catalog_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], policies: tuple[stove0_operator_contracts.AdmissionPolicyStatus, ...]) -> None"
    },
    "AdmissionPolicyStatus": {
      "kind": "class",
      "members": {
        "exact_policy": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "5f0c6158276ff3ee0f2695bf26ec3c7aa12e1b9341146ab950503d5efdf49ada",
      "signature": "(*, policy: stove0_operator_contracts.AdmissionPolicy, policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['new', 'baseline', 'following', 'reset_required'], source_identity: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, authorization_view_identity: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, baseline_mode: Literal['observe', 'backfill'], through_revision: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:0|[1-9][0-9]*)$')], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None"
    },
    "AdmissionRun": {
      "kind": "class",
      "schema_sha256": "6025bbb477e8e9a478cc47dd8e7d4a3d3ff478a918d32d17a35ba24e2a32a9e8",
      "signature": "(*, progressed: tuple[str, ...], failures: tuple[stove0_operator_contracts.SchedulerFailure, ...] = ()) -> None"
    },
    "AdmissionSort": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "AdmissionState": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "AdmissionView": {
      "kind": "class",
      "members": {
        "exact_stage": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "242e77047ed695bde61d11c1d145155b17b1c639bbfac0c8ad71f70f34d53339",
      "signature": "(*, intent: stove0_operator_contracts.AdmissionIntent, state: Literal['intent', 'previewed', 'work_bound'], preview_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, work_id: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, attempt_count: Annotated[int, Ge(ge=0)], next_attempt_at: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=40)] = None, failure: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=1000)] = None, created_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None"
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
    "BRANCH_SET_ADMITTED": {
      "kind": "constant",
      "value": "io.riverhog.stove0.branch-set.admitted"
    },
    "BranchSetAdmittedEvent": {
      "kind": "class",
      "schema_sha256": "e9f15dd111d9ab1a6351617077e89f91ae78aa273dc4f798930cd8ef66763cef",
      "signature": "(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.branch-set.admitted'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.BranchSetAdmittedEventData) -> None"
    },
    "BranchSetAdmittedEventData": {
      "kind": "class",
      "schema_sha256": "8915aca1dade95650ccfb03c5ac47cf18fb3cd693a2c2becad387ba7624688f6",
      "signature": "(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['coordinating'], revision: Annotated[int, Ge(ge=2)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], branch_count: Annotated[int, Ge(ge=1)], admitted_work_count: Annotated[int, Ge(ge=1)]) -> None"
    },
    "EVALUATION_CREATED": {
      "kind": "constant",
      "value": "io.riverhog.stove0.evaluation.created"
    },
    "EVALUATION_UPDATED": {
      "kind": "constant",
      "value": "io.riverhog.stove0.evaluation.updated"
    },
    "EvaluationChildView": {
      "kind": "class",
      "members": {
        "exact_output": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "d842ebe048284b0a96af0ab5da247cccef935c71e3371ed98e3fddbd3ce7e762",
      "signature": "(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['pending', 'active', 'complete', 'inapplicable', 'failed', 'canceled'], output: stove0_target_protocol.protocol.OutputCollectionRef | None = None) -> None"
    },
    "EvaluationCreatedEvent": {
      "kind": "class",
      "schema_sha256": "52570919e6a0bf09290e62d59812406cfb485c0add7710a05ffac8be63119baa",
      "signature": "(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.evaluation.created'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.EvaluationCreatedEventData) -> None"
    },
    "EvaluationCreatedEventData": {
      "kind": "class",
      "schema_sha256": "8a95a196acf8a76a51c6b3ef5fe4f5abffb52508d5830fac6aac0ff2138ae10a",
      "signature": "(*, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled']) -> None"
    },
    "EvaluationPage": {
      "kind": "class",
      "members": {
        "from_page": {
          "kind": "classmethod",
          "signature": "(cls, page: 'Mapping[str, Any]') -> 'EvaluationPage'"
        }
      },
      "schema_sha256": "00a56a81565ce9921f29db6856ca944ed6f8167189d80c36c596df55ec5c0950",
      "signature": "(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, sort: Literal['updated_at', 'phase', 'evaluation_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], evaluations: tuple[stove0_operator_contracts.EvaluationView, ...]) -> None"
    },
    "EvaluationPhase": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "EvaluationReviewIn": {
      "kind": "class",
      "members": {
        "meaningful": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "057c8f0be704962160a967eaa670c87ef7dd12dc49b77caed90b95a7d1b002fc",
      "signature": "(*, rating: Annotated[int | None, Ge(ge=1), Le(le=5)] = None, note: Annotated[Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\S(?:[\\\\s\\\\S]*\\\\S)?$', ascii_only=None)]], MaxLen(max_length=4000)] = None) -> None"
    },
    "EvaluationReviewView": {
      "kind": "class",
      "members": {
        "meaningful": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "5f6ace515468312142e6d3c1a30f7cec5850a1b90a5a169d4582ad88301f5e14",
      "signature": "(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], rating: Annotated[int | None, Ge(ge=1), Le(le=5)] = None, note: Annotated[Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\S(?:[\\\\s\\\\S]*\\\\S)?$', ascii_only=None)]], MaxLen(max_length=4000)] = None, updated_by: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None"
    },
    "EvaluationSort": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "EvaluationUpdatedEvent": {
      "kind": "class",
      "schema_sha256": "b4bc91ca87029c356c47ff9a7274db3f8f871bfa655b6696c04562571522aabc",
      "signature": "(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.evaluation.updated'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.EvaluationUpdatedEventData) -> None"
    },
    "EvaluationUpdatedEventData": {
      "kind": "class",
      "schema_sha256": "564ef285d77d3c9dad1bf60136e69ab8f2b535cfdc729c7830aa6ecdb46fd671",
      "signature": "(*, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=2)]) -> None"
    },
    "EvaluationView": {
      "kind": "class",
      "members": {
        "exact_identity": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        },
        "from_record": {
          "kind": "classmethod",
          "signature": "(cls, record: 'BaseModel | Mapping[str, Any]') -> 'EvaluationView'"
        }
      },
      "schema_sha256": "cac11576d6501d999cc52f3a6eb7b9749f72f524c0bb8797e80bc27396840637",
      "signature": "(*, format: Literal['stove0-evaluation-view/v1'] = 'stove0-evaluation-view/v1', evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], definition: stove0_protocol.models.EvaluationDefinition, phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=1)], children: tuple[stove0_operator_contracts.EvaluationChildView, ...], reviews: tuple[stove0_operator_contracts.EvaluationReviewView, ...] = ()) -> None"
    },
    "JOIN_ADMITTED": {
      "kind": "constant",
      "value": "io.riverhog.stove0.join.admitted"
    },
    "JoinAdmittedEvent": {
      "kind": "class",
      "schema_sha256": "0b6a8760c5eeffee01b5c936e89477f622b1f7e244905710451296fd4f0c7422",
      "signature": "(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.join.admitted'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.JoinAdmittedEventData) -> None"
    },
    "JoinAdmittedEventData": {
      "kind": "class",
      "schema_sha256": "dab536a055b1029ef8bc9d5c83589286aaeab4d73dbf9d5fc9a1f6b951f16e8d",
      "signature": "(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['coordinating'], revision: Annotated[int, Ge(ge=2)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "RecipeCatalogView": {
      "kind": "class",
      "schema_sha256": "08ee97d6df95f95a30d8ad8dd11a1688c3645e24bbed03251383f15e8a5e353c",
      "signature": "(*, catalog_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], recipes: tuple[stove0_operator_contracts.RecipeView, ...]) -> None"
    },
    "RecipeView": {
      "kind": "class",
      "members": {
        "exact_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        },
        "from_definition": {
          "kind": "classmethod",
          "signature": "(cls, definition: 'RecipeDefinition') -> 'RecipeView'"
        }
      },
      "schema_sha256": "7eee2b5e52261c8b2e7fee8a80d320dcd30aa7ff4511193c970ec212ddd62a4c",
      "signature": "(*, definition: stove0_recipe_config.models.RecipeDefinition, sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "STOVE0_EVENT_SOURCE": {
      "kind": "constant",
      "value": "urn:riverhog:stove0"
    },
    "STOVE0_EVENT_TYPES": {
      "kind": "constant",
      "value": [
        "io.riverhog.stove0.branch-set.admitted",
        "io.riverhog.stove0.evaluation.created",
        "io.riverhog.stove0.evaluation.updated",
        "io.riverhog.stove0.join.admitted",
        "io.riverhog.stove0.work.created",
        "io.riverhog.stove0.work.updated"
      ]
    },
    "STOVE0_HTTP_ERROR_AUTHORITY": {
      "kind": "object",
      "type": "http_api_contracts.HttpOperationErrorAuthority"
    },
    "SchedulerFailure": {
      "kind": "class",
      "members": {
        "one_subject": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "a833286ffda928b36d0a305881d883157303922614a8c4e55d7c3ff0134b32e2",
      "signature": "(*, event_id: str | None = None, work_id: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, error: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None"
    },
    "SchedulerPruning": {
      "kind": "class",
      "schema_sha256": "639614e3a2443f84fe63c2477757b5c11d526671cd5d37014cf1699993cccc8c",
      "signature": "(*, work: Annotated[int, Ge(ge=0)], work_bytes: Annotated[int, Ge(ge=0)], evaluations: Annotated[int, Ge(ge=0)], evaluation_bytes: Annotated[int, Ge(ge=0)], selections: Annotated[int, Ge(ge=0)], selection_bytes: Annotated[int, Ge(ge=0)], events: Annotated[int, Ge(ge=0)], event_bytes: Annotated[int, Ge(ge=0)]) -> None"
    },
    "SchedulerRole": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "SchedulerRun": {
      "kind": "class",
      "schema_sha256": "fee1ffcab5fa9cfca8ccd9dbcdb1291b4545b6a161c886b5f6b8402be1501bb2",
      "signature": "(*, pruning: stove0_operator_contracts.SchedulerPruning | None, admission: stove0_operator_contracts.AdmissionRun | None = None, work: stove0_operator_contracts.SchedulerWorkBatch) -> None"
    },
    "SchedulerRunIn": {
      "kind": "class",
      "schema_sha256": "bb6999171d296c228fdb5ff3ba85eb17a54fcbccdc9cfd7f0af37f0db3209dd5",
      "signature": "(*, role: Literal['controller', 'worker', 'combined'] = 'combined', work_limit: Annotated[int, Ge(ge=1), Le(le=100)] = 25) -> None"
    },
    "SchedulerStatus": {
      "kind": "class",
      "schema_sha256": "7a4eac0cdfe53ec109a9dd097dd163c7a16e9c52f9f04684ef04035f8d303bd5",
      "signature": "(*, running: bool, interval_seconds: Annotated[float, Gt(gt=0)], roles: tuple[typing.Literal['controller', 'worker', 'combined'], ...]) -> None"
    },
    "SchedulerWorkBatch": {
      "kind": "class",
      "schema_sha256": "84e8e1e2198f4b04c8d33af067097705e328f79c575320420d0034fca2160a60",
      "signature": "(*, role: Literal['controller', 'worker', 'combined'], cursor: str, next_cursor: str, progressed: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...], failures: tuple[stove0_operator_contracts.SchedulerFailure, ...]) -> None"
    },
    "SortOrder": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "Stove0CloudEvent": {
      "kind": "class",
      "members": {
        "exact_subject": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "0a714ffc7a24bde3a4c45a3624ec4bec304011f02c9cb8d6b0ec2cbeeb945700",
      "signature": "(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: Any) -> None"
    },
    "Stove0EventData": {
      "kind": "class",
      "members": {
        "get": {
          "kind": "method",
          "signature": "(self, key: 'str', default: 'Any' = None) -> 'Any'"
        }
      },
      "schema_sha256": "158157692a2d44d063a5dc475153416e4e4b4ab76f497b963fb959bdf83e9d03",
      "signature": "() -> None"
    },
    "Stove0EventPage": {
      "kind": "class",
      "members": {
        "require_progress_after": {
          "kind": "method",
          "signature": "(self, cursor: 'str') -> 'None'"
        }
      },
      "schema_sha256": "7dfcf567d2d57418b2348d851a596eaa26f7542d74fbbf52e84a54984b0e33ad",
      "signature": "(*, events: list[Stove0LifecycleEvent], next_cursor: str, has_more: bool) -> None"
    },
    "Stove0EventType": {
      "kind": "type-alias",
      "value": "typing.Literal['io.riverhog.stove0.work.created', 'io.riverhog.stove0.work.updated', 'io.riverhog.stove0.branch-set.admitted', 'io.riverhog.stove0.join.admitted', 'io.riverhog.stove0.evaluation.created', 'io.riverhog.stove0.evaluation.updated']"
    },
    "Stove0LifecycleEvent": {
      "kind": "type-alias",
      "value": "typing.Annotated[stove0_operator_contracts.WorkCreatedEvent | stove0_operator_contracts.WorkUpdatedEvent | stove0_operator_contracts.BranchSetAdmittedEvent | stove0_operator_contracts.JoinAdmittedEvent | stove0_operator_contracts.EvaluationCreatedEvent | stove0_operator_contracts.EvaluationUpdatedEvent, FieldInfo(annotation=NoneType, required=True, discriminator='type')]"
    },
    "WORK_CREATED": {
      "kind": "constant",
      "value": "io.riverhog.stove0.work.created"
    },
    "WORK_UPDATED": {
      "kind": "constant",
      "value": "io.riverhog.stove0.work.updated"
    },
    "WorkClaimView": {
      "kind": "class",
      "schema_sha256": "5836e31fafa2f07e9c5cb14ccd4562b4c1e719d136d58b41e3f193e6c594bc53",
      "signature": "(*, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)]) -> None"
    },
    "WorkCreateIn": {
      "kind": "class",
      "schema_sha256": "ea611476780c1760169ddacd75b984a7c9b035cf96394d12250b835495e6a748",
      "signature": "(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int | None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "WorkCreatedEvent": {
      "kind": "class",
      "schema_sha256": "14ba98a6cc43790e66a7c554f5956f029ada8c5216b7d5d71455bbf86d3500bf",
      "signature": "(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.work.created'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.WorkCreatedEventData) -> None"
    },
    "WorkCreatedEventData": {
      "kind": "class",
      "members": {
        "exact_parent_binding": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "e1e48763e34ffc2258b2a0ef13e61106e72258ba277b09a9263caad67af1c640",
      "signature": "(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'], parent_work_id: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, branch_set_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, join_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None) -> None"
    },
    "WorkFailureView": {
      "kind": "class",
      "schema_sha256": "3a61374424ae470a2c694ac13502faf98f91af05992638c02e1d5b5380dac0c4",
      "signature": "(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None"
    },
    "WorkInapplicableView": {
      "kind": "class",
      "schema_sha256": "6656e1447c29d937de4ad76ebaf92f3aee0431f6a1303726a004083da0376789",
      "signature": "(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None"
    },
    "WorkPage": {
      "kind": "class",
      "members": {
        "from_page": {
          "kind": "classmethod",
          "signature": "(cls, page: 'Mapping[str, Any]') -> 'WorkPage'"
        }
      },
      "schema_sha256": "d4dcd0f75a6790af3eececd4020f2a6b942bf376472f88997fc91584c8ce8095",
      "signature": "(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, sort: Literal['updated_at', 'phase', 'work_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], work: tuple[stove0_operator_contracts.WorkView, ...]) -> None"
    },
    "WorkPhase": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "WorkSort": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "WorkUpdatedEvent": {
      "kind": "class",
      "schema_sha256": "7acf51ea02c3f6916c1897ebba522acabdab1949ada2cf0aa9dd4ecc4865e666",
      "signature": "(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.work.updated'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.WorkUpdatedEventData) -> None"
    },
    "WorkUpdatedEventData": {
      "kind": "class",
      "schema_sha256": "ccdca8ddb59e6f6e51efd841d9b827e5b954d845a51a4146d07e9d1b2d80900a",
      "signature": "(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=2)]) -> None"
    },
    "WorkView": {
      "kind": "class",
      "members": {
        "exact_identity": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        },
        "from_record": {
          "kind": "classmethod",
          "signature": "(cls, record: 'BaseModel | Mapping[str, Any]') -> 'WorkView'"
        }
      },
      "schema_sha256": "4653eff1d6b27ef7d65f8577e7299e62b66330454054717075c27843f3dc1246",
      "signature": "(*, format: Literal['stove0-work-view/v1'] = 'stove0-work-view/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], work: stove0_protocol.models.WorkIdentity, phase: Literal['eligible', 'claimed', 'observing', 'planning', 'target_preflight', 'queued', 'executing', 'output_finalizing', 'verifying', 'settled', 'retirement_pending', 'coordinating', 'abandon_pending', 'complete', 'inapplicable', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=1)], claim: stove0_operator_contracts.WorkClaimView | None = None, preview_acceptance: stove0_operator_contracts.PreviewAcceptanceView | None = None, expected_target_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observation_requests: tuple[stove0_protocol.models.ObservationRequest, ...] = (), observation_results: tuple[stove0_protocol.models.ObservationResult, ...] = (), branch_set_plan: stove0_protocol.fork_join.BranchSetPlan | None = None, coordination_settlement: stove0_protocol.fork_join.CoordinationSettlement | None = None, join_plan: stove0_protocol.fork_join.JoinPlan | None = None, coordination_cancel_requested: bool = False, workflow_plan: stove0_protocol.models.WorkflowPlan | None = None, target_plan: Optional[Annotated[stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan, FieldInfo(annotation=NoneType, required=True, discriminator='protocol')]] = None, controller_evidence: stove0_protocol.models.ControllerEvidence | None = None, target_request: stove0_target_protocol.protocol.AcceptedTargetJob | None = None, target_status: stove0_target_protocol.protocol.TargetJobStatus | None = None, output: stove0_target_protocol.protocol.OutputCollectionRef | None = None, target_settlement: stove0_target_protocol.protocol.TargetSettlementAuthority | None = None, retirement_remaining: tuple[int, ...] = (), failure: stove0_operator_contracts.WorkFailureView | None = None, inapplicable: stove0_operator_contracts.WorkInapplicableView | None = None, abandon_outcome: Optional[Literal['inapplicable', 'failed', 'canceled']] = None) -> None"
    },
    "WorkflowPreviewIn": {
      "kind": "class",
      "members": {
        "canonical_inputs": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[CollectionRootRef, ...]') -> 'tuple[CollectionRootRef, ...]'"
        }
      },
      "schema_sha256": "d481dd8506429b4854eac08e0da41af254197bca3e7951499e90d837ad076ba9",
      "signature": "(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int | None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>) -> None"
    },
    "parse_stove0_event": {
      "kind": "function",
      "signature": "(value: 'object') -> 'Stove0LifecycleEvent'"
    },
    "stove0_event": {
      "kind": "function",
      "signature": "(*, type: 'Stove0EventType', subject: 'str', data: 'Mapping[str, Any]') -> 'Stove0LifecycleEvent'"
    },
    "validate_evaluation_child_shape": {
      "kind": "function",
      "signature": "(state: 'EvaluationChildState', output: 'OutputCollectionRef | None') -> 'None'"
    },
    "validate_evaluation_review_shape": {
      "kind": "function",
      "signature": "(rating: 'int | None', note: 'str | None') -> 'None'"
    },
    "validate_evaluation_state_shape": {
      "kind": "function",
      "signature": "(definition: 'EvaluationDefinition', children: 'Sequence[object]', reviews: 'Sequence[object]') -> 'None'"
    },
    "validate_work_state_shape": {
      "kind": "function",
      "signature": "(*, work: 'WorkIdentity', phase: 'WorkPhase', claim: 'object | None', preview_acceptance: 'object | None', expected_target_plan_sha256: 'str | None', branch_set_plan: 'BranchSetPlan | None', coordination_settlement: 'CoordinationSettlement | None', join_plan: 'JoinPlan | None', coordination_cancel_requested: 'bool', workflow_plan: 'WorkflowPlan | None', output: 'OutputCollectionRef | None', retirement_remaining: 'Sequence[int]', failure: 'object | None', inapplicable: 'object | None', abandon_outcome: \"Literal['inapplicable', 'failed', 'canceled'] | None\") -> 'None'"
    }
  },
  "module": "stove0_operator_contracts"
}
```
