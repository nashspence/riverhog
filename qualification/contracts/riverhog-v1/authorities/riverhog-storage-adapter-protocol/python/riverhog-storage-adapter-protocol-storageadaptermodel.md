# riverhog_storage_adapter_protocol.StorageAdapterModel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-8e40f5126d:387466f20a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4559792237"></a>
| Field | Shape |
|---|---|
| <a id="s-aec2f88ae4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-d83c972172"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-193c3b570c"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-e0f3e7613d"></a>`name` | "StorageAdapterModel" |
| <a id="s-020963594c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-8de292b84e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterModel`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fcc4b84fc3335dc02485709514a1d7320630b77cd63702a43ee5818728d4e5c5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "571e8620bee52a4520a602c0fda40f9631fb2fa449d9b858070ec0a5491c8add",
    "signature": "'() -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "StorageAdapterModel",
  "unit": "export"
}
```
