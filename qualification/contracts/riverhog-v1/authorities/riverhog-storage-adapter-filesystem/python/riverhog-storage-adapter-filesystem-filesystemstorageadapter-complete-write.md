# riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.complete_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-files-e502cc3ce9:096e6c5765 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-64de421f5e"></a>
- <a id="s-2a952e5b61"></a>`distribution`: `riverhog-storage-adapter-filesystem`
- <a id="s-3e94519767"></a>`module`: `riverhog_storage_adapter_filesystem`
- <a id="s-f1fdf55a4a"></a>`name`: `complete_write`
- <a id="s-b2be15c52b"></a>`owner`: `riverhog_storage_adapter_filesystem.FilesystemStorageAdapter`
- <a id="s-ce8aa249f7"></a>`unit`: `member`

### Declared structure

- <a id="s-2ac99b2a66"></a>`kind`: `"method"`
- <a id="s-184cf5c1b9"></a>`signature`: `"\"(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](riverhog-storage-adapter-filesystem-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-3267607881"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../../../evidence/sources.md#src-e075952170) — [reference/riverhog/storage/filesystem/src/riverhog\_storage\_adapter\_filesystem/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.complete_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b9555a1cb8269126e7c9f82360fb0518cf150f6db98fd426efcce873ae3a4be1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'\""
  },
  "distribution": "riverhog-storage-adapter-filesystem",
  "module": "riverhog_storage_adapter_filesystem",
  "name": "complete_write",
  "owner": "riverhog_storage_adapter_filesystem.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
