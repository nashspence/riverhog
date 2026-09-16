# riverhog_storage_adapter_support.StorageAdapterHttpBinding.handle

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-6ea739c5f5:096af7b498 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f4c0c87b4e"></a>
- <a id="s-7dfa30e4cd"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-8f610fb747"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-8dca27a125"></a>`name`: `handle`
- <a id="s-ddfd66f499"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterHttpBinding`
- <a id="s-7d1f1cda1b"></a>`unit`: `member`

### Declared structure

- <a id="s-3b71a00084"></a>`kind`: `"method"`
- <a id="s-ed45cc9fc2"></a>`signature`: `"\"(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'StorageAdapterHttpResponse'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterHttpBinding](riverhog-storage-adapter-support-storageadapterhttpbinding.md)

## Governing policies

- <a id="pa-28c266281e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterHttpBinding.handle`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6adc1e9724ca4e614d2aa6c26ec1d582405438b5f93c496b27916a55490def40 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'StorageAdapterHttpResponse'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "handle",
  "owner": "riverhog_storage_adapter_support.StorageAdapterHttpBinding",
  "unit": "member"
}
```

</details>
