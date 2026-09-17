# riverhog_storage_adapter_support.StorageAdapterClient.delete_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-354c065873:4a4ac32037 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-98749d3527"></a>
- <a id="s-c24b6b2510"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-c0691c0a0f"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-ee4b42df4f"></a>`name`: `delete_object`
- <a id="s-d5b8b2c8ab"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterClient`
- <a id="s-58b21f882a"></a>`unit`: `member`

### Declared structure

- <a id="s-918efe89ea"></a>`kind`: `"method"`
- <a id="s-1e3670a761"></a>`signature`: `"\"(self, request: 'DeleteObjectRequest') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-c028f0008f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.delete_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ab4bbc84ec416befa420b4891b3fb89b0729c36b786ea34cd7695254e54fd851 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'DeleteObjectRequest') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "delete_object",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

</details>
