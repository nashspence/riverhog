# riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.list_segments

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-files-95a523c98c:06db8ee528 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ce918f6ddf"></a>
- <a id="s-527452778a"></a>`distribution`: `riverhog-storage-adapter-filesystem`
- <a id="s-e5847d8c84"></a>`module`: `riverhog_storage_adapter_filesystem`
- <a id="s-2b8965ed21"></a>`name`: `list_segments`
- <a id="s-3ab4e9f9f5"></a>`owner`: `riverhog_storage_adapter_filesystem.FilesystemStorageAdapter`
- <a id="s-d45873050e"></a>`unit`: `member`

### Declared structure

- <a id="s-81067540d8"></a>`kind`: `"method"`
- <a id="s-3f53e6c7e9"></a>`signature`: `"\"(self, request: 'WriteSegmentListRequest') -> 'WriteSegmentPage'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](riverhog-storage-adapter-filesystem-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-d833592ca1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../../../evidence/sources.md#src-e075952170) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.list_segments`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37f4fa395d77ce3aac0182b716f372a2644234ecd3d3f9e35a027c7ff0dc6093 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteSegmentListRequest') -> 'WriteSegmentPage'\""
  },
  "distribution": "riverhog-storage-adapter-filesystem",
  "module": "riverhog_storage_adapter_filesystem",
  "name": "list_segments",
  "owner": "riverhog_storage_adapter_filesystem.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
