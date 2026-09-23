# a_riverhog_filesystem_store.FilesystemStorageAdapter.prepare_read

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-filesystem-store:a-riverhog-filesystem-store-filesystemsto-8218cdd7cd:8307484bb9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ebd09f920e"></a>
- <a id="s-fd6cec803d"></a>`distribution`: `a-riverhog-filesystem-store`
- <a id="s-84db0a1bfe"></a>`module`: `a_riverhog_filesystem_store`
- <a id="s-9e53acfcfd"></a>`name`: `prepare_read`
- <a id="s-e7dfbfd9fb"></a>`owner`: `a_riverhog_filesystem_store.FilesystemStorageAdapter`
- <a id="s-b32b1ce59d"></a>`unit`: `member`

### Declared structure

- <a id="s-5a4e623fda"></a>`kind`: `"method"`
- <a id="s-d9a001410a"></a>`signature`: `"\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](a-riverhog-filesystem-store-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-552f2fd1e9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-filesystem-store:a_riverhog_filesystem_store](../../../evidence/sources/authorities.md#src-d49395fbda) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_filesystem_store.FilesystemStorageAdapter.prepare_read`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b8a8951d66070cafb7d3f9d6518c862f2c7f28032a94f3f2a262d0e58f70023d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""
  },
  "distribution": "a-riverhog-filesystem-store",
  "module": "a_riverhog_filesystem_store",
  "name": "prepare_read",
  "owner": "a_riverhog_filesystem_store.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
