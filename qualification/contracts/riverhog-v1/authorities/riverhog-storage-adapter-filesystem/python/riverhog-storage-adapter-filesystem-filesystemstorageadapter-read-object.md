# riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.read_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-files-25bbf201fb:0e08c00b7c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c73fced9f"></a>
- <a id="s-fadfd7d127"></a>`distribution`: `riverhog-storage-adapter-filesystem`
- <a id="s-e580bcca7d"></a>`module`: `riverhog_storage_adapter_filesystem`
- <a id="s-471a80aaf4"></a>`name`: `read_object`
- <a id="s-e5a155f694"></a>`owner`: `riverhog_storage_adapter_filesystem.FilesystemStorageAdapter`
- <a id="s-13f5a8f966"></a>`unit`: `member`

### Declared structure

- <a id="s-2ffdf977d8"></a>`kind`: `"method"`
- <a id="s-ec0ab36cd7"></a>`signature`: `"\"(self, request: 'ObjectReadRequest') -> 'ObjectReadStream'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](riverhog-storage-adapter-filesystem-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-e738d80577"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../../../evidence/sources/authorities.md#src-e075952170) — [reference/riverhog/storage/filesystem/src/riverhog\_storage\_adapter\_filesystem/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.read_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0d77641658171afe5e5b03b446d192c0482434c13c4ab7828b8c58eb8221069d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ObjectReadRequest') -> 'ObjectReadStream'\""
  },
  "distribution": "riverhog-storage-adapter-filesystem",
  "module": "riverhog_storage_adapter_filesystem",
  "name": "read_object",
  "owner": "riverhog_storage_adapter_filesystem.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
