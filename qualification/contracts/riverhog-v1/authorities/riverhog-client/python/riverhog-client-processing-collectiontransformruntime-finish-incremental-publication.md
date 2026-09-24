# riverhog_client.processing.CollectionTransformRuntime.finish_incremental_publication

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-collectiontran-b39d9538c9:7caf50f628 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9ea37bf871"></a>
- <a id="s-31a3657f0c"></a>`distribution`: `riverhog-client`
- <a id="s-448f5b9fb5"></a>`module`: `riverhog_client.processing`
- <a id="s-03e898afba"></a>`name`: `finish_incremental_publication`
- <a id="s-4c127bc561"></a>`owner`: `riverhog_client.processing.CollectionTransformRuntime`
- <a id="s-0edbd0d205"></a>`unit`: `member`

### Declared structure

- <a id="s-3603deab6b"></a>`kind`: `"method"`
- <a id="s-983664ccc8"></a>`signature`: `"\"(self, writer: 'IncrementalDerivedCollectionWriter', *, execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', **kwargs: 'Any') -> 'DerivedCollectionReceipt'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-processing-collectiontransformruntime.md)

## Governing policies

- <a id="pa-afb6be5c91"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CollectionTransformRuntime.finish_incremental_publication`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f8389a35131c9ac00ba0cd19d6f065f9725cd0ae41060099057b85c29743ffa -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, writer: 'IncrementalDerivedCollectionWriter', *, execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', **kwargs: 'Any') -> 'DerivedCollectionReceipt'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "finish_incremental_publication",
  "owner": "riverhog_client.processing.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
