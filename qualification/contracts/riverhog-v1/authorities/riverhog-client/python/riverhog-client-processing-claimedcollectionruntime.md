# riverhog_client.processing.ClaimedCollectionRuntime

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollectionruntime:2525c04fcd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a701c029de"></a>
- <a id="s-bff6723c88"></a>`distribution`: `riverhog-client`
- <a id="s-78ee402100"></a>`module`: `riverhog_client.processing`
- <a id="s-d1df62a879"></a>`name`: `ClaimedCollectionRuntime`
- <a id="s-73694dce6b"></a>`unit`: `export`

### Declared structure

- <a id="s-1c1b63bd7c"></a>`kind`: `"class"`
- <a id="s-f79bc5e744"></a>`signature`: `"'(api: \\'Any\\', *, inputs: \\'Sequence[CollectionRootIdentity]\\', claim_id: \\'str\\', fence: \\'int\\', work_id: \\'str\\', execution_id: \\'str\\', cancellation_check: \\'CancellationCheck \| None\\' = None, input_retrieval_policy: \"Literal[\\'available-only\\', \\'allow\\']\" = \\'available-only\\', owned_api: \\'bool\\' = False) -> \\'None\\''"`

## Maintained corroboration

### Related interface records

- [heartbeat](riverhog-client-processing-claimedcollectionruntime-heartbeat.md)
- [prepare_inputs](riverhog-client-processing-claimedcollectionruntime-prepare-inputs.md)
- [from_capability](riverhog-client-processing-claimedcollectionruntime-from-capability.md)
- [iter_inventory](riverhog-client-processing-claimedcollectionruntime-iter-inventory.md)
- [refresh_capability](riverhog-client-processing-claimedcollectionruntime-refresh-capability.md)
- [__enter__](riverhog-client-processing-claimedcollectionruntime-enter.md)
- [open_workspace](riverhog-client-processing-claimedcollectionruntime-open-workspace.md)
- [__exit__](riverhog-client-processing-claimedcollectionruntime-exit.md)
- [close](riverhog-client-processing-claimedcollectionruntime-close.md)

## Governing policies

- <a id="pa-68bdadf3df"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionRuntime`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c220fd151aa89283145f3950134a5d1c3dc9d5ea45f7a4b07ea5068692789a13 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(api: \\'Any\\', *, inputs: \\'Sequence[CollectionRootIdentity]\\', claim_id: \\'str\\', fence: \\'int\\', work_id: \\'str\\', execution_id: \\'str\\', cancellation_check: \\'CancellationCheck | None\\' = None, input_retrieval_policy: \"Literal[\\'available-only\\', \\'allow\\']\" = \\'available-only\\', owned_api: \\'bool\\' = False) -> \\'None\\''"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "ClaimedCollectionRuntime",
  "unit": "export"
}
```

</details>
