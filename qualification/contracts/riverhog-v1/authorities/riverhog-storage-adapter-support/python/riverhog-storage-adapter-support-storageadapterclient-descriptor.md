# riverhog_storage_adapter_support.StorageAdapterClient.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-0aa5affe86:12db70cc68 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4d444cf46b"></a>
- <a id="s-d1eadd1b00"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-bb15c840c7"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-d1a9555d56"></a>`name`: `descriptor`
- <a id="s-06b60ed60f"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterClient`
- <a id="s-cf2dce9b3c"></a>`unit`: `member`

### Declared structure

- <a id="s-7b70080eb1"></a>`kind`: `"method"`
- <a id="s-ed3add158e"></a>`signature`: `"\"(self) -> 'AdapterDescriptor'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-f324ca8080"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 31d16c02e6044790f1769878489a44b2791ad6f943639d5dc8d32caab3e574b4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'AdapterDescriptor'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "descriptor",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

</details>
