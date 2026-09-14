# riverhog_storage_adapter_support.StorageAdapterClient.head_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-db186a64e7:a766d9001b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a463305ee0"></a>
- <a id="s-86370f526f"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-a6d1f833ca"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-6bded25cf4"></a>`name`: `head_object`
- <a id="s-622f22e2f0"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterClient`
- <a id="s-f32a842476"></a>`unit`: `member`

### Declared structure

- <a id="s-b93fef1294"></a>`kind`: `"method"`
- <a id="s-5159b2e0a8"></a>`signature`: `"\"(self, request: 'ObjectHeadRequest') -> 'ObjectMetadataReceipt \| None'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_support.StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-64a2d0d869"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.head_object`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bea1c1059c564d0d7051888c472d598c315e670984401b312a076229c038bb96 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ObjectHeadRequest') -> 'ObjectMetadataReceipt | None'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "head_object",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```
