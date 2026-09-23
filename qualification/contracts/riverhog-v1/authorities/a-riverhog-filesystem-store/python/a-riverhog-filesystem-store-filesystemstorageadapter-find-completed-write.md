# a_riverhog_filesystem_store.FilesystemStorageAdapter.find_completed_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-filesystem-store:a-riverhog-filesystem-store-filesystemsto-c3848803d3:c7c21d2e3b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-928ae8bbbe"></a>
- <a id="s-107e6c5566"></a>`distribution`: `a-riverhog-filesystem-store`
- <a id="s-90b969cf27"></a>`module`: `a_riverhog_filesystem_store`
- <a id="s-6944255d02"></a>`name`: `find_completed_write`
- <a id="s-683f144a74"></a>`owner`: `a_riverhog_filesystem_store.FilesystemStorageAdapter`
- <a id="s-0b56830180"></a>`unit`: `member`

### Declared structure

- <a id="s-55cce5a9b7"></a>`kind`: `"method"`
- <a id="s-02b3335b5a"></a>`signature`: `"\"(self, request: 'CompletedWriteLookupRequest') -> 'CompletedObjectReceipt \| None'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](a-riverhog-filesystem-store-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-cafc62bc78"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-filesystem-store:a_riverhog_filesystem_store](../../../evidence/sources/authorities.md#src-d49395fbda) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_filesystem_store.FilesystemStorageAdapter.find_completed_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8332d8255b0048369112fff7798d27f96e58bbf88896975e3e4d58a382ec1da8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'CompletedWriteLookupRequest') -> 'CompletedObjectReceipt | None'\""
  },
  "distribution": "a-riverhog-filesystem-store",
  "module": "a_riverhog_filesystem_store",
  "name": "find_completed_write",
  "owner": "a_riverhog_filesystem_store.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
