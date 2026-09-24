# riverhog_client.processing.CollectionTransformRuntime

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-collectiontran-9956676bc1:94c86f3213 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-61e8f8123e"></a>
- <a id="s-2e885e6293"></a>`distribution`: `riverhog-client`
- <a id="s-8f2c5e0fd8"></a>`module`: `riverhog_client.processing`
- <a id="s-583fa3a317"></a>`name`: `CollectionTransformRuntime`
- <a id="s-cdaec24296"></a>`unit`: `export`

### Declared structure

- <a id="s-afe3166228"></a>`kind`: `"class"`
- <a id="s-c267d24a2a"></a>`signature`: `"'(api: \\'Any\\', *, spec: \\'DerivedCollectionSpec\\', claim_id: \\'str\\', fence: \\'int\\', work_id: \\'str\\', execution_id: \\'str\\', controller_evidence: \\'Mapping[str, object]\\', producer_app: \\'str\\', producer_version: \\'str\\' = \\'development\\', cancellation_check: \\'CancellationCheck \| None\\' = None, input_retrieval_policy: \"Literal[\\'available-only\\', \\'allow\\']\" = \\'available-only\\', owned_api: \\'bool\\' = False) -> \\'None\\''"`

## Maintained corroboration

### Related interface records

- [open_workspace](riverhog-client-processing-collectiontransformruntime-open-workspace.md)
- [__exit__](riverhog-client-processing-collectiontransformruntime-exit.md)
- [heartbeat](riverhog-client-processing-collectiontransformruntime-heartbeat.md)
- [append_incremental_output](riverhog-client-processing-collectiontransformruntime-append-incremental-output.md)
- [from_capability](riverhog-client-processing-collectiontransformruntime-from-capability.md)
- [open_incremental_publication](riverhog-client-processing-collectiontransformruntime-open-incremental-publication.md)
- [prepare_inputs](riverhog-client-processing-collectiontransformruntime-prepare-inputs.md)
- [refresh_capability](riverhog-client-processing-collectiontransformruntime-refresh-capability.md)
- [finish_incremental_publication](riverhog-client-processing-collectiontransformruntime-finish-incremental-publication.md)
- [iter_inventory](riverhog-client-processing-collectiontransformruntime-iter-inventory.md)
- [close](riverhog-client-processing-collectiontransformruntime-close.md)
- [publish](riverhog-client-processing-collectiontransformruntime-publish.md)
- [__enter__](riverhog-client-processing-collectiontransformruntime-enter.md)

## Governing policies

- <a id="pa-f602f39b13"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CollectionTransformRuntime`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9120510e2720597161e11c3233fa505ca0d1e35eba1f3ed065452445b5ba1660 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(api: \\'Any\\', *, spec: \\'DerivedCollectionSpec\\', claim_id: \\'str\\', fence: \\'int\\', work_id: \\'str\\', execution_id: \\'str\\', controller_evidence: \\'Mapping[str, object]\\', producer_app: \\'str\\', producer_version: \\'str\\' = \\'development\\', cancellation_check: \\'CancellationCheck | None\\' = None, input_retrieval_policy: \"Literal[\\'available-only\\', \\'allow\\']\" = \\'available-only\\', owned_api: \\'bool\\' = False) -> \\'None\\''"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "CollectionTransformRuntime",
  "unit": "export"
}
```

</details>
