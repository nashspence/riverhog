# a_riverhog_filesystem_store.FilesystemStorageAdapter.delete_prefix

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-filesystem-store:a-riverhog-filesystem-store-filesystemsto-49d8c05133:504c3c99ad -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-81d674f067"></a>
- <a id="s-985f8acd15"></a>`distribution`: `a-riverhog-filesystem-store`
- <a id="s-034c6994b7"></a>`module`: `a_riverhog_filesystem_store`
- <a id="s-6037f512e0"></a>`name`: `delete_prefix`
- <a id="s-01d0dfdcef"></a>`owner`: `a_riverhog_filesystem_store.FilesystemStorageAdapter`
- <a id="s-125023c6d1"></a>`unit`: `member`

### Declared structure

- <a id="s-0850bd32a7"></a>`kind`: `"method"`
- <a id="s-80fa107172"></a>`signature`: `"\"(self, request: 'DeletePrefixRequest') -> 'int'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](a-riverhog-filesystem-store-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-06b742dee6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-filesystem-store:a_riverhog_filesystem_store](../../../evidence/sources/authorities.md#src-d49395fbda) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_filesystem_store.FilesystemStorageAdapter.delete_prefix`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d371c7c4f9ef63a6fe6fa85f1188602b463742d6bf22f4162853edf5bc5f1ab6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'DeletePrefixRequest') -> 'int'\""
  },
  "distribution": "a-riverhog-filesystem-store",
  "module": "a_riverhog_filesystem_store",
  "name": "delete_prefix",
  "owner": "a_riverhog_filesystem_store.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
