# riverhog_storage_adapter_support.StorageAdapterProtocolError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-4fab9bc76b:418f278e68 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b4b6a808f9"></a>
- <a id="s-49c8ad0674"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-e4d7cf433f"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-0b148db92a"></a>`name`: `StorageAdapterProtocolError`
- <a id="s-431339566b"></a>`unit`: `export`

### Declared structure

- <a id="s-285653ef93"></a>`kind`: `"class"`
- <a id="s-950842adb0"></a>`signature`: `"\"(message: 'str', *, status_code: 'int \| None' = None, code: 'StorageAdapterErrorCode' = 'internal_failure') -> 'None'\""`

## Governing policies

- <a id="pa-bb372389f1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterProtocolError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e2f9c00fa948b99adf20ed7e6a95b40e44ed18edee73ae7a8b237df8fb24e42 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, status_code: 'int | None' = None, code: 'StorageAdapterErrorCode' = 'internal_failure') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "StorageAdapterProtocolError",
  "unit": "export"
}
```

</details>
