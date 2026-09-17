# riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-files-b81c41024a:b210859eac -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b02b53d486"></a>
- <a id="s-c4ad2012a0"></a>`distribution`: `riverhog-storage-adapter-filesystem`
- <a id="s-cad4335aa7"></a>`module`: `riverhog_storage_adapter_filesystem`
- <a id="s-5139909ad1"></a>`name`: `__exit__`
- <a id="s-0fe099c19b"></a>`owner`: `riverhog_storage_adapter_filesystem.FilesystemStorageAdapter`
- <a id="s-bb8b31d8c5"></a>`unit`: `member`

### Declared structure

- <a id="s-c324a74a6a"></a>`kind`: `"method"`
- <a id="s-70fb73b962"></a>`signature`: `"\"(self, *_: 'object') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](riverhog-storage-adapter-filesystem-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-0f39d16544"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../../../evidence/sources/authorities.md#src-e075952170) — [reference/riverhog/storage/filesystem/src/riverhog\_storage\_adapter\_filesystem/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 129e560690a074a984ad886de449b25037a5acc592d215af06b7c1b10c1caa2e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *_: 'object') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-filesystem",
  "module": "riverhog_storage_adapter_filesystem",
  "name": "__exit__",
  "owner": "riverhog_storage_adapter_filesystem.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
