# riverhog_client.transform.ClaimedCollectionRuntime

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollectionruntime:db87c87597 -->

Exact externally visible contract owned by this semantic dossier.

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

- [riverhog_client.transform.ClaimedCollectionRuntime.__enter__](riverhog-client-transform-claimedcollectionruntime-enter.md)
- [riverhog_client.transform.ClaimedCollectionRuntime.prepare_inputs](riverhog-client-transform-claimedcollectionruntime-prepare-inputs.md)
- [riverhog_client.transform.ClaimedCollectionRuntime.__exit__](riverhog-client-transform-claimedcollectionruntime-exit.md)
- [riverhog_client.transform.ClaimedCollectionRuntime.open_workspace](riverhog-client-transform-claimedcollectionruntime-open-workspace.md)
- [riverhog_client.transform.ClaimedCollectionRuntime.refresh_capability](riverhog-client-transform-claimedcollectionruntime-refresh-capability.md)
- [riverhog_client.transform.ClaimedCollectionRuntime.iter_inventory](riverhog-client-transform-claimedcollectionruntime-iter-inventory.md)
- [riverhog_client.transform.ClaimedCollectionRuntime.heartbeat](riverhog-client-transform-claimedcollectionruntime-heartbeat.md)
- [riverhog_client.transform.ClaimedCollectionRuntime.close](riverhog-client-transform-claimedcollectionruntime-close.md)
- [riverhog_client.transform.ClaimedCollectionRuntime.from_capability](riverhog-client-transform-claimedcollectionruntime-from-capability.md)

## Governing policies

- <a id="pa-0c99421322"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionRuntime`

### Exact owned JSON

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
