# riverhog_client.transform.ClaimedCollectionRuntime

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollectionruntime:db87c87597 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-287d4b6b0a"></a>
- <a id="s-b4217334fb"></a>`distribution`: `riverhog-client`
- <a id="s-c76f95d696"></a>`module`: `riverhog_client.transform`
- <a id="s-1ab0df14ec"></a>`name`: `ClaimedCollectionRuntime`
- <a id="s-fdaf4f575e"></a>`unit`: `export`

### Declared structure

- <a id="s-0397d0be25"></a>`kind`: `"class"`
- <a id="s-d05b69bbde"></a>`signature`: `"'(api: \\'Any\\', *, inputs: \\'Sequence[CollectionRootIdentity]\\', claim_id: \\'str\\', fence: \\'int\\', work_id: \\'str\\', execution_id: \\'str\\', cancellation_check: \\'CancellationCheck \| None\\' = None, input_retrieval_policy: \"Literal[\\'available-only\\', \\'allow\\']\" = \\'available-only\\', owned_api: \\'bool\\' = False) -> \\'None\\''"`

## Maintained corroboration

### Related interface records

- [__enter__](riverhog-client-transform-claimedcollectionruntime-enter.md)
- [prepare_inputs](riverhog-client-transform-claimedcollectionruntime-prepare-inputs.md)
- [__exit__](riverhog-client-transform-claimedcollectionruntime-exit.md)
- [open_workspace](riverhog-client-transform-claimedcollectionruntime-open-workspace.md)
- [refresh_capability](riverhog-client-transform-claimedcollectionruntime-refresh-capability.md)
- [iter_inventory](riverhog-client-transform-claimedcollectionruntime-iter-inventory.md)
- [heartbeat](riverhog-client-transform-claimedcollectionruntime-heartbeat.md)
- [close](riverhog-client-transform-claimedcollectionruntime-close.md)
- [from_capability](riverhog-client-transform-claimedcollectionruntime-from-capability.md)

## Governing policies

- <a id="pa-0c99421322"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionRuntime`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7d65ed7d4348765f5338dbdf51de0eed63818e466e9b7170352764bc16162935 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(api: \\'Any\\', *, inputs: \\'Sequence[CollectionRootIdentity]\\', claim_id: \\'str\\', fence: \\'int\\', work_id: \\'str\\', execution_id: \\'str\\', cancellation_check: \\'CancellationCheck | None\\' = None, input_retrieval_policy: \"Literal[\\'available-only\\', \\'allow\\']\" = \\'available-only\\', owned_api: \\'bool\\' = False) -> \\'None\\''"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "ClaimedCollectionRuntime",
  "unit": "export"
}
```

</details>
