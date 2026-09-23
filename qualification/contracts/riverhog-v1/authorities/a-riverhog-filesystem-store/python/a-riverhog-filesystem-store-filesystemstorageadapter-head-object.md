# a_riverhog_filesystem_store.FilesystemStorageAdapter.head_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-filesystem-store:a-riverhog-filesystem-store-filesystemsto-977ea95cc4:573b02a490 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-91ad9a4eb8"></a>
- <a id="s-69af361cc7"></a>`distribution`: `a-riverhog-filesystem-store`
- <a id="s-e62c910548"></a>`module`: `a_riverhog_filesystem_store`
- <a id="s-b3e0ede029"></a>`name`: `head_object`
- <a id="s-72384c9e4a"></a>`owner`: `a_riverhog_filesystem_store.FilesystemStorageAdapter`
- <a id="s-11c5fdddb2"></a>`unit`: `member`

### Declared structure

- <a id="s-ddca1d8494"></a>`kind`: `"method"`
- <a id="s-6e1207b2a4"></a>`signature`: `"\"(self, request: 'ObjectHeadRequest') -> 'ObjectMetadataReceipt \| None'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](a-riverhog-filesystem-store-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-5e66f71996"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-filesystem-store:a_riverhog_filesystem_store](../../../evidence/sources/authorities.md#src-d49395fbda) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_filesystem_store.FilesystemStorageAdapter.head_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2372f6d5c013c612ae6200190b9653a9b207e230a129e5bc41708dff6e4e7d6d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ObjectHeadRequest') -> 'ObjectMetadataReceipt | None'\""
  },
  "distribution": "a-riverhog-filesystem-store",
  "module": "a_riverhog_filesystem_store",
  "name": "head_object",
  "owner": "a_riverhog_filesystem_store.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
