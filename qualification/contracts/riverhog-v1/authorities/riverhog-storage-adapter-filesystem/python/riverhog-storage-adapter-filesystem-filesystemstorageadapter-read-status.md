# riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.read_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-files-d7aaa999e8:9961240c7c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6fcf6c5c2c"></a>
- <a id="s-1846266c23"></a>`distribution`: `riverhog-storage-adapter-filesystem`
- <a id="s-3b3ea8e083"></a>`module`: `riverhog_storage_adapter_filesystem`
- <a id="s-c2c5b40777"></a>`name`: `read_status`
- <a id="s-302179a063"></a>`owner`: `riverhog_storage_adapter_filesystem.FilesystemStorageAdapter`
- <a id="s-3b5ee2cd0d"></a>`unit`: `member`

### Declared structure

- <a id="s-9fdf6a0e55"></a>`kind`: `"method"`
- <a id="s-c3fb0ea13a"></a>`signature`: `"\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter](riverhog-storage-adapter-filesystem-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-dcbb4c37f0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../../../evidence/sources.md#src-e075952170) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.read_status`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e5e3d6394461caf028d95b4a82dc5031cba9a88118821854c906342819c255a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""
  },
  "distribution": "riverhog-storage-adapter-filesystem",
  "module": "riverhog_storage_adapter_filesystem",
  "name": "read_status",
  "owner": "riverhog_storage_adapter_filesystem.FilesystemStorageAdapter",
  "unit": "member"
}
```
