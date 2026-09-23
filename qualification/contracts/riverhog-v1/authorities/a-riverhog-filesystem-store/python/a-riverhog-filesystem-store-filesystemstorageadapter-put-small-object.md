# a_riverhog_filesystem_store.FilesystemStorageAdapter.put_small_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-filesystem-store:a-riverhog-filesystem-store-filesystemsto-7f52cd94eb:977f18e4eb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-11121b9fea"></a>
- <a id="s-6ce33907c0"></a>`distribution`: `a-riverhog-filesystem-store`
- <a id="s-cbbd34ec81"></a>`module`: `a_riverhog_filesystem_store`
- <a id="s-e5b74c30cd"></a>`name`: `put_small_object`
- <a id="s-9c5d04939a"></a>`owner`: `a_riverhog_filesystem_store.FilesystemStorageAdapter`
- <a id="s-e54632f207"></a>`unit`: `member`

### Declared structure

- <a id="s-56239b3c96"></a>`kind`: `"method"`
- <a id="s-81f4246d85"></a>`signature`: `"\"(self, request: 'SmallObjectWriteRequest', content: 'BinaryContent') -> 'ImmutableObjectReceipt'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](a-riverhog-filesystem-store-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-9b133a4b28"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-filesystem-store:a_riverhog_filesystem_store](../../../evidence/sources/authorities.md#src-d49395fbda) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_filesystem_store.FilesystemStorageAdapter.put_small_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8d67c328d7747c05291f00e0a776b878b4f0f3f27c5fc5445f47124096252c8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'SmallObjectWriteRequest', content: 'BinaryContent') -> 'ImmutableObjectReceipt'\""
  },
  "distribution": "a-riverhog-filesystem-store",
  "module": "a_riverhog_filesystem_store",
  "name": "put_small_object",
  "owner": "a_riverhog_filesystem_store.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
