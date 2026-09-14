# riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.cleanup_read

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-files-ad2ee356c0:fe54b19e1d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bba8f5cd56"></a>
- <a id="s-4c30e85acd"></a>`distribution`: `riverhog-storage-adapter-filesystem`
- <a id="s-810379a52c"></a>`module`: `riverhog_storage_adapter_filesystem`
- <a id="s-20d395fc04"></a>`name`: `cleanup_read`
- <a id="s-03dc323bd9"></a>`owner`: `riverhog_storage_adapter_filesystem.FilesystemStorageAdapter`
- <a id="s-4580cd47ae"></a>`unit`: `member`

### Declared structure

- <a id="s-5574ab6d55"></a>`kind`: `"method"`
- <a id="s-621b144335"></a>`signature`: `"\"(self, request: 'ReadPreparationRequest') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter](riverhog-storage-adapter-filesystem-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-2df48b7190"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../../../evidence/sources.md#src-e075952170) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.cleanup_read`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e342c026ee1b3b3ef392a1648aacdd71b4f07f8037683a942200da948c73f80 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ReadPreparationRequest') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-filesystem",
  "module": "riverhog_storage_adapter_filesystem",
  "name": "cleanup_read",
  "owner": "riverhog_storage_adapter_filesystem.FilesystemStorageAdapter",
  "unit": "member"
}
```
