# stove0_target_support.TargetExecutionRuntime

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionruntime:402d5713ea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4d678ee895"></a>
- <a id="s-14cff5ec57"></a>`distribution`: `stove0-target-support`
- <a id="s-f7a326839e"></a>`module`: `stove0_target_support`
- <a id="s-8e7f4c367c"></a>`name`: `TargetExecutionRuntime`
- <a id="s-4562ab88b5"></a>`unit`: `export`

### Declared structure

- <a id="s-1fd7af19b6"></a>`kind`: `"class"`
- <a id="s-a1d01082e1"></a>`signature`: `"\"(request: 'TargetJobRequest', runtime: 'ClaimedCollectionRuntime \| CollectionTransformRuntime', *, session: 'TargetExecutionSession \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [iter_inputs](stove0-target-support-targetexecutionruntime-iter-inputs.md)
- [effect_success](stove0-target-support-targetexecutionruntime-effect-success.md)
- [open_collection_publication](stove0-target-support-targetexecutionruntime-open-collection-publication.md)
- [resolve_input_ids](stove0-target-support-targetexecutionruntime-resolve-input-ids.md)
- [refresh_capability](stove0-target-support-targetexecutionruntime-refresh-capability.md)
- [open_workspace](stove0-target-support-targetexecutionruntime-open-workspace.md)
- [completed](stove0-target-support-targetexecutionruntime-completed.md)
- [prepare_inputs](stove0-target-support-targetexecutionruntime-prepare-inputs.md)
- [declare_disposition](stove0-target-support-targetexecutionruntime-declare-disposition.md)
- [from_request](stove0-target-support-targetexecutionruntime-from-request.md)
- [__enter__](stove0-target-support-targetexecutionruntime-enter.md)
- [__exit__](stove0-target-support-targetexecutionruntime-exit.md)
- [job_id](stove0-target-support-targetexecutionruntime-job-id.md)

## Governing policies

- <a id="pa-5af557bc2e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5923c518dc070efe188675e4009e38cb350e41a3ca5b04ba0ec4b192b0507319 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(request: 'TargetJobRequest', runtime: 'ClaimedCollectionRuntime | CollectionTransformRuntime', *, session: 'TargetExecutionSession | None' = None) -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetExecutionRuntime",
  "unit": "export"
}
```

</details>
