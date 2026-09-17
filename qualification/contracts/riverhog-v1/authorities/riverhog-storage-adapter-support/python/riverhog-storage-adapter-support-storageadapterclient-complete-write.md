# riverhog_storage_adapter_support.StorageAdapterClient.complete_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-c690f5c531:1ca5acd53e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d5a049982b"></a>
- <a id="s-d0f52fa227"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-99c3a46d40"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-f4d7ef7609"></a>`name`: `complete_write`
- <a id="s-5ae90abbe7"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterClient`
- <a id="s-b289e3d5b1"></a>`unit`: `member`

### Declared structure

- <a id="s-110d589ef9"></a>`kind`: `"method"`
- <a id="s-f092275d8d"></a>`signature`: `"\"(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-b3c0f0b6cc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.complete_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e7a3ac23057752fc6ba07b84a2652ad47b7e719f0255dddf1ebe1162555deb8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "complete_write",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

</details>
