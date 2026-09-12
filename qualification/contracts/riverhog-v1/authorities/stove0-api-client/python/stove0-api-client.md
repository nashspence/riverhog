# stove0_api_client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client:0462b65b31 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-17756ca4c599) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c5068a926233"></a>
| Field | Shape |
|---|---|
| <a id="s-41c4b7a2a853"></a>`distribution` | "stove0-api-client" |
| <a id="s-e99d9519a23e"></a>`exports` | additional keys=`HealthResponse`, `Stove0ApiClient`, `Stove0ApiError` |
| <a id="s-4c78c7f6fa21"></a>`module` | "stove0_api_client" |

## Governing policies

- <a id="pa-aa456a333ae3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba506)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client](../../../evidence/sources.md#src-f412d556d5a6) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py::<module>`

### Machine authority

- `/external_contract/python/15`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba848215faf2b58126d647cc900aa0152c19f0717d4cbe72b28465425ac6fbd0 -->

```json
{
  "distribution": "stove0-api-client",
  "exports": {
    "HealthResponse": {
      "kind": "class",
      "schema_sha256": "873f58b65973a85d82bd4e352acd595a8f32f6058c4500f11514358669b42b31",
      "signature": "(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None"
    },
    "Stove0ApiClient": {
      "kind": "class",
      "members": {
        "backfill_admission_policy": {
          "kind": "method",
          "signature": "(self, policy_id: 'str') -> 'AdmissionPolicyStatus'"
        },
        "cancel_evaluation": {
          "kind": "method",
          "signature": "(self, evaluation_id: 'str') -> 'EvaluationView'"
        },
        "cancel_work": {
          "kind": "method",
          "signature": "(self, work_id: 'str') -> 'WorkView'"
        },
        "close": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "create_evaluation": {
          "kind": "method",
          "signature": "(self, definition: 'Mapping[str, Any]') -> 'EvaluationView'"
        },
        "create_work": {
          "kind": "method",
          "signature": "(self, recipe_id: 'str', inputs: 'Sequence[CollectionRootRef]', *, preview_sha256: 'str', recipe_revision: 'int | None' = None, effective_intent: 'Mapping[str, Any] | None' = None) -> 'WorkView'"
        },
        "get_admission": {
          "kind": "method",
          "signature": "(self, admission_id: 'str') -> 'AdmissionView'"
        },
        "get_artifact_selection": {
          "kind": "method",
          "signature": "(self, selection_sha256: 'str', *, continuation: 'str | None' = None) -> 'ArtifactSelectionPage'"
        },
        "get_evaluation": {
          "kind": "method",
          "signature": "(self, evaluation_id: 'str') -> 'EvaluationView'"
        },
        "get_recipe": {
          "kind": "method",
          "signature": "(self, recipe_id: 'str', *, revision: 'int | None' = None) -> 'RecipeView'"
        },
        "get_work": {
          "kind": "method",
          "signature": "(self, work_id: 'str') -> 'WorkView'"
        },
        "health_live": {
          "kind": "method",
          "signature": "(self) -> 'HealthResponse'"
        },
        "health_ready": {
          "kind": "method",
          "signature": "(self) -> 'HealthResponse'"
        },
        "inspect_work_coordination": {
          "kind": "method",
          "signature": "(self, work_id: 'str') -> 'BranchSetEvaluation'"
        },
        "list_admission_policies": {
          "kind": "method",
          "signature": "(self) -> 'AdmissionPolicyCatalogView'"
        },
        "list_admissions": {
          "kind": "method",
          "signature": "(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, policy_id: 'str | None' = None, state: 'AdmissionState | None' = None, query: 'str | None' = None, sort: 'AdmissionSort' = 'created_at', order: 'SortOrder' = 'desc') -> 'AdmissionPage'"
        },
        "list_evaluations": {
          "kind": "method",
          "signature": "(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, phase: 'EvaluationPhase | None' = None, query: 'str | None' = None, sort: 'EvaluationSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'EvaluationPage'"
        },
        "list_events": {
          "kind": "method",
          "signature": "(self, *, after: 'str | None' = None, limit: 'int' = 100) -> 'Stove0EventPage'"
        },
        "list_recipes": {
          "kind": "method",
          "signature": "(self) -> 'RecipeCatalogView'"
        },
        "list_work": {
          "kind": "method",
          "signature": "(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, phase: 'WorkPhase | None' = None, query: 'str | None' = None, sort: 'WorkSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'WorkPage'"
        },
        "preview_workflow": {
          "kind": "method",
          "signature": "(self, recipe_id: 'str', inputs: 'Sequence[CollectionRootRef]', *, recipe_revision: 'int | None' = None, effective_intent: 'Mapping[str, Any] | None' = None) -> 'WorkflowPreview'"
        },
        "rebaseline_admission_policy": {
          "kind": "method",
          "signature": "(self, policy_id: 'str') -> 'AdmissionPolicyStatus'"
        },
        "retry_evaluation_variant": {
          "kind": "method",
          "signature": "(self, evaluation_id: 'str', variant_id: 'str') -> 'EvaluationView'"
        },
        "retry_work": {
          "kind": "method",
          "signature": "(self, work_id: 'str') -> 'WorkView'"
        },
        "review_evaluation_variant": {
          "kind": "method",
          "signature": "(self, evaluation_id: 'str', variant_id: 'str', *, rating: 'int | None' = None, note: 'str | None' = None) -> 'EvaluationView'"
        },
        "run_scheduler": {
          "kind": "method",
          "signature": "(self, *, role: 'SchedulerRole' = 'combined', work_limit: 'int' = 25) -> 'SchedulerRun'"
        },
        "scheduler_status": {
          "kind": "method",
          "signature": "(self) -> 'SchedulerStatus'"
        },
        "step_evaluation": {
          "kind": "method",
          "signature": "(self, evaluation_id: 'str') -> 'EvaluationView'"
        },
        "step_work": {
          "kind": "method",
          "signature": "(self, work_id: 'str') -> 'WorkView'"
        }
      },
      "signature": "(base_url: 'str | None' = None, token: 'str | None' = None, *, allow_insecure_http: 'bool | None' = None, timeout_seconds: 'float | None' = None, http2: 'bool | None' = None) -> 'None'"
    },
    "Stove0ApiError": {
      "kind": "class",
      "signature": "(message: 'str', *, code: 'str' = 'stove0_client_error', observed_status: 'int | None' = None, details: 'Mapping[str, Any] | None' = None) -> 'None'"
    }
  },
  "module": "stove0_api_client"
}
```
