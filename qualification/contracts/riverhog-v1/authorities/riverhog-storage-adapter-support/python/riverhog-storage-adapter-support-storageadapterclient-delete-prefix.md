# riverhog_storage_adapter_support.StorageAdapterClient.delete_prefix

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-e8afd9cc05:7513c40118 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ed7980f110"></a>
- <a id="s-c67bfd741b"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-25bffa9e81"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-0c98c4f4ff"></a>`name`: `delete_prefix`
- <a id="s-28692e0147"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterClient`
- <a id="s-29044b6ca7"></a>`unit`: `member`

### Declared structure

- <a id="s-6be4f921bb"></a>`kind`: `"method"`
- <a id="s-cda99da488"></a>`signature`: `"\"(self, request: 'DeletePrefixRequest') -> 'int'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-03e51ce942"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources/authorities.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.delete_prefix`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba06e19debfe345308280c79e736740dc9caec33c548f9359a9b4f9ebb7ef7c8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'DeletePrefixRequest') -> 'int'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "delete_prefix",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

</details>
