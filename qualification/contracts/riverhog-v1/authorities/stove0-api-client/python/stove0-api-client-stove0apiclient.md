# stove0_api_client.Stove0ApiClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient:436c93e928 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c609e504a3"></a>
- <a id="s-647dfcb6db"></a>`distribution`: `stove0-api-client`
- <a id="s-24b8372ca9"></a>`module`: `stove0_api_client`
- <a id="s-9814cbd1e6"></a>`name`: `Stove0ApiClient`
- <a id="s-a748fa4b78"></a>`unit`: `export`

### Declared structure

- <a id="s-ffb0ef1216"></a>`kind`: `"class"`
- <a id="s-48cc3ed84e"></a>`signature`: `"\"(base_url: 'str \| None' = None, token: 'str \| None' = None, *, allow_insecure_http: 'bool \| None' = None, timeout_seconds: 'float \| None' = None, http2: 'bool \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [backfill_admission_policy](stove0-api-client-stove0apiclient-backfill-admission-policy.md)
- [cancel_evaluation](stove0-api-client-stove0apiclient-cancel-evaluation.md)
- [cancel_work](stove0-api-client-stove0apiclient-cancel-work.md)
- [close](stove0-api-client-stove0apiclient-close.md)
- [create_evaluation](stove0-api-client-stove0apiclient-create-evaluation.md)
- [create_work](stove0-api-client-stove0apiclient-create-work.md)
- [__enter__](stove0-api-client-stove0apiclient-enter.md)
- [__exit__](stove0-api-client-stove0apiclient-exit.md)
- [get_admission](stove0-api-client-stove0apiclient-get-admission.md)
- [get_artifact_selection](stove0-api-client-stove0apiclient-get-artifact-selection.md)
- [get_departure_effect](stove0-api-client-stove0apiclient-get-departure-effect.md)
- [get_evaluation](stove0-api-client-stove0apiclient-get-evaluation.md)
- [get_recipe](stove0-api-client-stove0apiclient-get-recipe.md)
- [get_work](stove0-api-client-stove0apiclient-get-work.md)
- [health_live](stove0-api-client-stove0apiclient-health-live.md)
- [health_ready](stove0-api-client-stove0apiclient-health-ready.md)
- [inspect_work_coordination](stove0-api-client-stove0apiclient-inspect-work-coordination.md)
- [list_admission_policies](stove0-api-client-stove0apiclient-list-admission-policies.md)
- [list_admissions](stove0-api-client-stove0apiclient-list-admissions.md)
- [list_departure_policies](stove0-api-client-stove0apiclient-list-departure-policies.md)
- [list_departure_effects](stove0-api-client-stove0apiclient-list-departure-effects.md)
- [list_evaluations](stove0-api-client-stove0apiclient-list-evaluations.md)
- [list_events](stove0-api-client-stove0apiclient-list-events.md)
- [list_recipes](stove0-api-client-stove0apiclient-list-recipes.md)
- [list_work](stove0-api-client-stove0apiclient-list-work.md)
- [preview_workflow](stove0-api-client-stove0apiclient-preview-workflow.md)
- [rebaseline_departure_policy](stove0-api-client-stove0apiclient-rebaseline-departure-policy.md)
- [rebaseline_admission_policy](stove0-api-client-stove0apiclient-rebaseline-admission-policy.md)
- [retry_evaluation_variant](stove0-api-client-stove0apiclient-retry-evaluation-variant.md)
- [retry_work](stove0-api-client-stove0apiclient-retry-work.md)
- [review_evaluation_variant](stove0-api-client-stove0apiclient-review-evaluation-variant.md)
- [run_scheduler](stove0-api-client-stove0apiclient-run-scheduler.md)
- [scheduler_status](stove0-api-client-stove0apiclient-scheduler-status.md)
- [step_evaluation](stove0-api-client-stove0apiclient-step-evaluation.md)
- [step_work](stove0-api-client-stove0apiclient-step-work.md)

## Governing policies

- <a id="pa-d5b95f3f9a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
