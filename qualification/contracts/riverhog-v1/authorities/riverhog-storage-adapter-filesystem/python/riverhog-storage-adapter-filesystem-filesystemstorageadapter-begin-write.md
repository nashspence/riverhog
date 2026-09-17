# riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.begin_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-files-b193bb3192:bed19e1f8d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d22f490dcc"></a>
- <a id="s-53e2b56580"></a>`distribution`: `riverhog-storage-adapter-filesystem`
- <a id="s-c189122c5f"></a>`module`: `riverhog_storage_adapter_filesystem`
- <a id="s-44f3e98808"></a>`name`: `begin_write`
- <a id="s-e28c2694c2"></a>`owner`: `riverhog_storage_adapter_filesystem.FilesystemStorageAdapter`
- <a id="s-a1f6f77fc5"></a>`unit`: `member`

### Declared structure

- <a id="s-5cf57043ae"></a>`kind`: `"method"`
- <a id="s-dd10e9f4db"></a>`signature`: `"\"(self, request: 'WriteStartRequest') -> 'WriteSession'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](riverhog-storage-adapter-filesystem-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-b9fa0f2e1c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../../../evidence/sources.md#src-e075952170) — [reference/riverhog/storage/filesystem/src/riverhog\_storage\_adapter\_filesystem/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.begin_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82d24eb28d97f04f920ec6a6e57a237019df0a65a5b430e4d5b9635be72b9d1d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteStartRequest') -> 'WriteSession'\""
  },
  "distribution": "riverhog-storage-adapter-filesystem",
  "module": "riverhog_storage_adapter_filesystem",
  "name": "begin_write",
  "owner": "riverhog_storage_adapter_filesystem.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
