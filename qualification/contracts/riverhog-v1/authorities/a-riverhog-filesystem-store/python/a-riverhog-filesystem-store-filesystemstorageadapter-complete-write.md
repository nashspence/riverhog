# a_riverhog_filesystem_store.FilesystemStorageAdapter.complete_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-filesystem-store:a-riverhog-filesystem-store-filesystemsto-312b9a48a3:156ab38fb4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b8286fe26a"></a>
- <a id="s-64e654b1a2"></a>`distribution`: `a-riverhog-filesystem-store`
- <a id="s-85cc6938da"></a>`module`: `a_riverhog_filesystem_store`
- <a id="s-553ae8c302"></a>`name`: `complete_write`
- <a id="s-50563fffba"></a>`owner`: `a_riverhog_filesystem_store.FilesystemStorageAdapter`
- <a id="s-3a429b4cbe"></a>`unit`: `member`

### Declared structure

- <a id="s-991f38641d"></a>`kind`: `"method"`
- <a id="s-28bbe6af99"></a>`signature`: `"\"(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](a-riverhog-filesystem-store-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-0f53514400"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-filesystem-store:a_riverhog_filesystem_store](../../../evidence/sources/authorities.md#src-d49395fbda) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_filesystem_store.FilesystemStorageAdapter.complete_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c84b445fffe108ab21b7ebcb55399ef968a16ebd388d94d5aad3d7f293158117 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'\""
  },
  "distribution": "a-riverhog-filesystem-store",
  "module": "a_riverhog_filesystem_store",
  "name": "complete_write",
  "owner": "a_riverhog_filesystem_store.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
