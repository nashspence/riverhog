# riverhog_storage_adapter_support.StorageAdapterHttpBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-715ecca009:82f2d1abc8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3494399e69"></a>
- <a id="s-cdfa43750a"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-c7a607aa7d"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-429333be55"></a>`name`: `StorageAdapterHttpBinding`
- <a id="s-0305773b6a"></a>`unit`: `export`

### Declared structure

- <a id="s-f5102d96bb"></a>`kind`: `"class"`
- <a id="s-07a6639f42"></a>`signature`: `"\"(adapter: 'StorageAdapterPort', *, maximum_control_bytes: 'int' = 67108864) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_support.StorageAdapterHttpBinding.handle_framed](riverhog-storage-adapter-support-storageadapterhttpbinding-handle-framed.md)
- [riverhog_storage_adapter_support.StorageAdapterHttpBinding.handle](riverhog-storage-adapter-support-storageadapterhttpbinding-handle.md)

## Governing policies

- <a id="pa-ec7d040c90"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterHttpBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 85c10b8ff5f72a589192f972e66572c064acfaf7cc950b60cf77580f79397c0c -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(adapter: 'StorageAdapterPort', *, maximum_control_bytes: 'int' = 67108864) -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "StorageAdapterHttpBinding",
  "unit": "export"
}
```
