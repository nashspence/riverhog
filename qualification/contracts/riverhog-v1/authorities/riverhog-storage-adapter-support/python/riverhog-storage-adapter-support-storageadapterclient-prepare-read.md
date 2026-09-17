# riverhog_storage_adapter_support.StorageAdapterClient.prepare_read

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-61651790ae:c3df10dc76 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cfe39929c3"></a>
- <a id="s-941206f10e"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-9472f328ae"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-21973052cf"></a>`name`: `prepare_read`
- <a id="s-fa152f63cb"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterClient`
- <a id="s-af69d0a7b1"></a>`unit`: `member`

### Declared structure

- <a id="s-5dc7f69a6f"></a>`kind`: `"method"`
- <a id="s-0e5e1be3c6"></a>`signature`: `"\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-59182906a8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.prepare_read`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d6775dd2c644067a1656e41407d00ea83146e5e281e1a2cc8773c5c6bf83e10d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "prepare_read",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

</details>
