# stove0_api_client.Stove0ApiClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient:436c93e928 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c609e504a3"></a>
| Field | Shape |
|---|---|
| <a id="s-d421a95a4b"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-647dfcb6db"></a>`distribution` | "stove0-api-client" |
| <a id="s-24b8372ca9"></a>`module` | "stove0_api_client" |
| <a id="s-9814cbd1e6"></a>`name` | "Stove0ApiClient" |
| <a id="s-a748fa4b78"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_api_client.Stove0ApiClient.backfill_admission_policy](stove0-api-client-stove0apiclient-backfill-admission-policy.md)
- [stove0_api_client.Stove0ApiClient.cancel_evaluation](stove0-api-client-stove0apiclient-cancel-evaluation.md)
- [stove0_api_client.Stove0ApiClient.cancel_work](stove0-api-client-stove0apiclient-cancel-work.md)
- [stove0_api_client.Stove0ApiClient.close](stove0-api-client-stove0apiclient-close.md)
- [stove0_api_client.Stove0ApiClient.create_evaluation](stove0-api-client-stove0apiclient-create-evaluation.md)
- [stove0_api_client.Stove0ApiClient.create_work](stove0-api-client-stove0apiclient-create-work.md)
- [stove0_api_client.Stove0ApiClient.__enter__](stove0-api-client-stove0apiclient-enter.md)
- [stove0_api_client.Stove0ApiClient.__exit__](stove0-api-client-stove0apiclient-exit.md)
- [stove0_api_client.Stove0ApiClient.get_admission](stove0-api-client-stove0apiclient-get-admission.md)
- [stove0_api_client.Stove0ApiClient.get_artifact_selection](stove0-api-client-stove0apiclient-get-artifact-selection.md)
- [stove0_api_client.Stove0ApiClient.get_evaluation](stove0-api-client-stove0apiclient-get-evaluation.md)
- [stove0_api_client.Stove0ApiClient.get_recipe](stove0-api-client-stove0apiclient-get-recipe.md)
- [stove0_api_client.Stove0ApiClient.get_work](stove0-api-client-stove0apiclient-get-work.md)
- [stove0_api_client.Stove0ApiClient.health_live](stove0-api-client-stove0apiclient-health-live.md)
- [stove0_api_client.Stove0ApiClient.health_ready](stove0-api-client-stove0apiclient-health-ready.md)
- [stove0_api_client.Stove0ApiClient.inspect_work_coordination](stove0-api-client-stove0apiclient-inspect-work-coordination.md)
- [stove0_api_client.Stove0ApiClient.list_admission_policies](stove0-api-client-stove0apiclient-list-admission-policies.md)
- [stove0_api_client.Stove0ApiClient.list_admissions](stove0-api-client-stove0apiclient-list-admissions.md)
- [stove0_api_client.Stove0ApiClient.list_evaluations](stove0-api-client-stove0apiclient-list-evaluations.md)
- [stove0_api_client.Stove0ApiClient.list_events](stove0-api-client-stove0apiclient-list-events.md)
- [stove0_api_client.Stove0ApiClient.list_recipes](stove0-api-client-stove0apiclient-list-recipes.md)
- [stove0_api_client.Stove0ApiClient.list_work](stove0-api-client-stove0apiclient-list-work.md)
- [stove0_api_client.Stove0ApiClient.preview_workflow](stove0-api-client-stove0apiclient-preview-workflow.md)
- [stove0_api_client.Stove0ApiClient.rebaseline_admission_policy](stove0-api-client-stove0apiclient-rebaseline-admission-policy.md)
- [stove0_api_client.Stove0ApiClient.retry_evaluation_variant](stove0-api-client-stove0apiclient-retry-evaluation-variant.md)
- [stove0_api_client.Stove0ApiClient.retry_work](stove0-api-client-stove0apiclient-retry-work.md)
- [stove0_api_client.Stove0ApiClient.review_evaluation_variant](stove0-api-client-stove0apiclient-review-evaluation-variant.md)
- [stove0_api_client.Stove0ApiClient.run_scheduler](stove0-api-client-stove0apiclient-run-scheduler.md)
- [stove0_api_client.Stove0ApiClient.scheduler_status](stove0-api-client-stove0apiclient-scheduler-status.md)
- [stove0_api_client.Stove0ApiClient.step_evaluation](stove0-api-client-stove0apiclient-step-evaluation.md)
- [stove0_api_client.Stove0ApiClient.step_work](stove0-api-client-stove0apiclient-step-work.md)

## Governing policies

- <a id="pa-d5b95f3f9a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd43c0d4992106769ca10b77e781d9b19d858ce152fe433c56846cf3bacf71e9 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(base_url: 'str | None' = None, token: 'str | None' = None, *, allow_insecure_http: 'bool | None' = None, timeout_seconds: 'float | None' = None, http2: 'bool | None' = None) -> 'None'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "Stove0ApiClient",
  "unit": "export"
}
```
