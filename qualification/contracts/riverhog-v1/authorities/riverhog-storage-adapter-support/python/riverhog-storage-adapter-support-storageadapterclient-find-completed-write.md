# riverhog_storage_adapter_support.StorageAdapterClient.find_completed_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-70dd2a8e82:a8770726a4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4e1732b480"></a>
- <a id="s-0e99e66cbe"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-e8ddbd7c4c"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-61fb46cd67"></a>`name`: `find_completed_write`
- <a id="s-7469dd4d9e"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterClient`
- <a id="s-264167e6bd"></a>`unit`: `member`

### Declared structure

- <a id="s-e3aac7dd9b"></a>`kind`: `"method"`
- <a id="s-3d386c1725"></a>`signature`: `"\"(self, request: 'CompletedWriteLookupRequest') -> 'CompletedObjectReceipt \| None'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-d79314f793"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources/authorities.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.find_completed_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b822852b1d5f482a80ef83fd62320ef7f97dcb4fd6c2ff71d57cdafca1eb8937 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'CompletedWriteLookupRequest') -> 'CompletedObjectReceipt | None'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "find_completed_write",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

</details>
