# riverhog_storage_adapter_protocol.StorageAdapterError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-2d8aa31d10:20ca0a401e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-339d4e2876"></a>
| Field | Shape |
|---|---|
| <a id="s-cd5662d56f"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-1df02dc29f"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-b959e15e1d"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-2fd6a49f10"></a>`name` | "StorageAdapterError" |
| <a id="s-69541978c7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-44a4df69cb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterError`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0d93567819aa9a0fda3f3dcae41c79d59c30ed2f4b0cf6f616ba91343e4cee8b -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "19b906afcdf8d94d77f5aef65b4facf0cb084d382ef94d440ae8a5cd58e7aa7f",
    "signature": "'(*, error: riverhog_storage_adapter_protocol.protocol.StorageAdapterErrorBody) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "StorageAdapterError",
  "unit": "export"
}
```
