# a_riverhog_filesystem_store.FilesystemStorageAdapter.read_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-filesystem-store:a-riverhog-filesystem-store-filesystemsto-8f71c3ead9:8fd7ee6f98 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0eec8f1c74"></a>
- <a id="s-fab53e7d74"></a>`distribution`: `a-riverhog-filesystem-store`
- <a id="s-f103ae2482"></a>`module`: `a_riverhog_filesystem_store`
- <a id="s-9fff565ae4"></a>`name`: `read_status`
- <a id="s-7c8d01f823"></a>`owner`: `a_riverhog_filesystem_store.FilesystemStorageAdapter`
- <a id="s-4f43760303"></a>`unit`: `member`

### Declared structure

- <a id="s-84b77b7127"></a>`kind`: `"method"`
- <a id="s-adb22e5a9b"></a>`signature`: `"\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](a-riverhog-filesystem-store-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-eb00e73beb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-filesystem-store:a_riverhog_filesystem_store](../../../evidence/sources/authorities.md#src-d49395fbda) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_filesystem_store.FilesystemStorageAdapter.read_status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40ecc05ee85df1a7a88292400a256d5d5cef59cf467208ab1bbd2069c54e2bbd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""
  },
  "distribution": "a-riverhog-filesystem-store",
  "module": "a_riverhog_filesystem_store",
  "name": "read_status",
  "owner": "a_riverhog_filesystem_store.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
