# riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-files-fb1632ea07:52e43c28cc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4133c8667f"></a>
- <a id="s-a416aa0dae"></a>`distribution`: `riverhog-storage-adapter-filesystem`
- <a id="s-56aacd7eeb"></a>`module`: `riverhog_storage_adapter_filesystem`
- <a id="s-494f41611b"></a>`name`: `descriptor`
- <a id="s-d38a80ccf6"></a>`owner`: `riverhog_storage_adapter_filesystem.FilesystemStorageAdapter`
- <a id="s-fd1aca1b0c"></a>`unit`: `member`

### Declared structure

- <a id="s-bb9597e063"></a>`kind`: `"method"`
- <a id="s-612000e60f"></a>`signature`: `"\"(self) -> 'AdapterDescriptor'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter](riverhog-storage-adapter-filesystem-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-ba14ea9578"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../../../evidence/sources.md#src-e075952170) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.descriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9117a9410d1e5d3c190207f1baded45154b8c8299bc4b49041e6ebaf19bc1478 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'AdapterDescriptor'\""
  },
  "distribution": "riverhog-storage-adapter-filesystem",
  "module": "riverhog_storage_adapter_filesystem",
  "name": "descriptor",
  "owner": "riverhog_storage_adapter_filesystem.FilesystemStorageAdapter",
  "unit": "member"
}
```
