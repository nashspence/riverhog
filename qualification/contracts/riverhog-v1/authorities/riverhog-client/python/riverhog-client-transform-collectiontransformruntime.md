# riverhog_client.transform.CollectionTransformRuntime

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-collectiontransformruntime:4a1612c698 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-250deae17a"></a>
- <a id="s-b07ef9b0ff"></a>`distribution`: `riverhog-client`
- <a id="s-e2e7071e79"></a>`module`: `riverhog_client.transform`
- <a id="s-b6288359a2"></a>`name`: `CollectionTransformRuntime`
- <a id="s-c4b5b70f21"></a>`unit`: `export`

### Declared structure

- <a id="s-e0cf59816e"></a>`kind`: `"class"`
- <a id="s-351d481a33"></a>`signature`: `"'(api: \\'Any\\', *, spec: \\'DerivedCollectionSpec\\', claim_id: \\'str\\', fence: \\'int\\', work_id: \\'str\\', execution_id: \\'str\\', controller_evidence: \\'Mapping[str, object]\\', producer_app: \\'str\\', producer_version: \\'str\\' = \\'development\\', cancellation_check: \\'CancellationCheck \| None\\' = None, input_retrieval_policy: \"Literal[\\'available-only\\', \\'allow\\']\" = \\'available-only\\', owned_api: \\'bool\\' = False) -> \\'None\\''"`

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.CollectionTransformRuntime.open_workspace](riverhog-client-transform-collectiontransformruntime-open-workspace.md)
- [riverhog_client.transform.CollectionTransformRuntime.from_capability](riverhog-client-transform-collectiontransformruntime-from-capability.md)
- [riverhog_client.transform.CollectionTransformRuntime.iter_inventory](riverhog-client-transform-collectiontransformruntime-iter-inventory.md)
- [riverhog_client.transform.CollectionTransformRuntime.append_incremental_output](riverhog-client-transform-collectiontransformruntime-append-incremental-output.md)
- [riverhog_client.transform.CollectionTransformRuntime.open_incremental_publication](riverhog-client-transform-collectiontransformruntime-open-incremental-publication.md)
- [riverhog_client.transform.CollectionTransformRuntime.publish](riverhog-client-transform-collectiontransformruntime-publish.md)
- [riverhog_client.transform.CollectionTransformRuntime.__enter__](riverhog-client-transform-collectiontransformruntime-enter.md)
- [riverhog_client.transform.CollectionTransformRuntime.close](riverhog-client-transform-collectiontransformruntime-close.md)
- [riverhog_client.transform.CollectionTransformRuntime.refresh_capability](riverhog-client-transform-collectiontransformruntime-refresh-capability.md)
- [riverhog_client.transform.CollectionTransformRuntime.finish_incremental_publication](riverhog-client-transform-collectiontransformruntime-finish-incremental-publication.md)
- [riverhog_client.transform.CollectionTransformRuntime.heartbeat](riverhog-client-transform-collectiontransformruntime-heartbeat.md)
- [riverhog_client.transform.CollectionTransformRuntime.__exit__](riverhog-client-transform-collectiontransformruntime-exit.md)
- [riverhog_client.transform.CollectionTransformRuntime.prepare_inputs](riverhog-client-transform-collectiontransformruntime-prepare-inputs.md)

## Governing policies

- <a id="pa-908b9c04af"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.CollectionTransformRuntime`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc8cd98057bbb89fdc3fca9393ad75dd4f386a7ded1ef97695348cc99c3c3476 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(api: \\'Any\\', *, spec: \\'DerivedCollectionSpec\\', claim_id: \\'str\\', fence: \\'int\\', work_id: \\'str\\', execution_id: \\'str\\', controller_evidence: \\'Mapping[str, object]\\', producer_app: \\'str\\', producer_version: \\'str\\' = \\'development\\', cancellation_check: \\'CancellationCheck | None\\' = None, input_retrieval_policy: \"Literal[\\'available-only\\', \\'allow\\']\" = \\'available-only\\', owned_api: \\'bool\\' = False) -> \\'None\\''"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "CollectionTransformRuntime",
  "unit": "export"
}
```
