# riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.readiness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-files-a3c42aaa11:d73531f891 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-907b51f15c"></a>
- <a id="s-5f6f96295a"></a>`distribution`: `riverhog-storage-adapter-filesystem`
- <a id="s-2514c258f9"></a>`module`: `riverhog_storage_adapter_filesystem`
- <a id="s-8b050f1199"></a>`name`: `readiness`
- <a id="s-b41827320e"></a>`owner`: `riverhog_storage_adapter_filesystem.FilesystemStorageAdapter`
- <a id="s-f1bdda3bb4"></a>`unit`: `member`

### Declared structure

- <a id="s-2dc269fd95"></a>`kind`: `"method"`
- <a id="s-3f822359e9"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](riverhog-storage-adapter-filesystem-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-a9c32b1aa4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../../../evidence/sources.md#src-e075952170) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.readiness`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dcba7f66106c63e8f8720236cff9e7f56149b5d7946906bed9247aef20393653 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-filesystem",
  "module": "riverhog_storage_adapter_filesystem",
  "name": "readiness",
  "owner": "riverhog_storage_adapter_filesystem.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
