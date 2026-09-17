# riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.write_segment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-files-a8d2081d60:12c3130c5d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-965b2c5054"></a>
- <a id="s-fabddae0ed"></a>`distribution`: `riverhog-storage-adapter-filesystem`
- <a id="s-36d71c5127"></a>`module`: `riverhog_storage_adapter_filesystem`
- <a id="s-19d708d903"></a>`name`: `write_segment`
- <a id="s-03f2b2e25a"></a>`owner`: `riverhog_storage_adapter_filesystem.FilesystemStorageAdapter`
- <a id="s-6a0517dd31"></a>`unit`: `member`

### Declared structure

- <a id="s-12c00a6cc5"></a>`kind`: `"method"`
- <a id="s-ff90c05c1e"></a>`signature`: `"\"(self, *, session: 'WriteSession', number: 'int', stored_bytes: 'int', content: 'BinaryContent') -> 'WriteSegmentReceipt'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](riverhog-storage-adapter-filesystem-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-3d5e2324d4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../../../evidence/sources/authorities.md#src-e075952170) — [reference/riverhog/storage/filesystem/src/riverhog\_storage\_adapter\_filesystem/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.write_segment`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0977efa5a7ce75967f46e92caa6ab0951ebb7d6d640528e29432eba59b9f0c7c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, session: 'WriteSession', number: 'int', stored_bytes: 'int', content: 'BinaryContent') -> 'WriteSegmentReceipt'\""
  },
  "distribution": "riverhog-storage-adapter-filesystem",
  "module": "riverhog_storage_adapter_filesystem",
  "name": "write_segment",
  "owner": "riverhog_storage_adapter_filesystem.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
