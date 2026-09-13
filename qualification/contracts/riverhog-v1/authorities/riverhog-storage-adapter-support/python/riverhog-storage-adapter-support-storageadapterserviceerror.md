# riverhog_storage_adapter_support.StorageAdapterServiceError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-0781e326c1:0f9e5024d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3530f0561b"></a>
| Field | Shape |
|---|---|
| <a id="s-f6c0858a83"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-fee533bd11"></a>`distribution` | "riverhog-storage-adapter-support" |
| <a id="s-7152753f67"></a>`module` | "riverhog_storage_adapter_support" |
| <a id="s-e1d3ac25da"></a>`name` | "StorageAdapterServiceError" |
| <a id="s-d91b746852"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c59ce1a0cd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterServiceError`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ed606b0eaf9b2de1fd669a008853a593c10dc708e765e8b8b3f2ad133fa8bbfc -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(status: 'int', code: 'StorageAdapterErrorCode', message: 'str') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "StorageAdapterServiceError",
  "unit": "export"
}
```
