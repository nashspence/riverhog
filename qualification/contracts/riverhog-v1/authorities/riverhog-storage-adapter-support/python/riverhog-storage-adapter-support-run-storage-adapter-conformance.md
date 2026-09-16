# riverhog_storage_adapter_support.run_storage_adapter_conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-run-stor-cdad78e9f5:f37fc9dc9e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1336f0c9f2"></a>
- <a id="s-0bfc510997"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-d369812718"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-c4efde9aea"></a>`name`: `run_storage_adapter_conformance`
- <a id="s-f02a170a6a"></a>`unit`: `export`

### Declared structure

- <a id="s-c077af53aa"></a>`kind`: `"function"`
- <a id="s-40af8f4f2b"></a>`signature`: `"\"(client: 'StorageAdapterClient', *, continuation_client: 'StorageAdapterClient', object_prefix: 'str') -> 'StorageAdapterConformanceResult'\""`

## Governing policies

- <a id="pa-f64fa1f5e8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.run_storage_adapter_conformance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23b45d98c245f6902f0d914641889762ef74d580f51c4ae04ec71696bf2fbc5c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(client: 'StorageAdapterClient', *, continuation_client: 'StorageAdapterClient', object_prefix: 'str') -> 'StorageAdapterConformanceResult'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "run_storage_adapter_conformance",
  "unit": "export"
}
```

</details>
