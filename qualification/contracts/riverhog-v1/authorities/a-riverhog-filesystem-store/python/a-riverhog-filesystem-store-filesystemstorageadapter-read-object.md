# a_riverhog_filesystem_store.FilesystemStorageAdapter.read_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-filesystem-store:a-riverhog-filesystem-store-filesystemsto-e2777d558a:1640c04076 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-76c50e68db"></a>
- <a id="s-0a30646dc9"></a>`distribution`: `a-riverhog-filesystem-store`
- <a id="s-cd5dc010b6"></a>`module`: `a_riverhog_filesystem_store`
- <a id="s-d7b98687ae"></a>`name`: `read_object`
- <a id="s-e62679b978"></a>`owner`: `a_riverhog_filesystem_store.FilesystemStorageAdapter`
- <a id="s-538e95e42e"></a>`unit`: `member`

### Declared structure

- <a id="s-f890bf6c7a"></a>`kind`: `"method"`
- <a id="s-c899256ac0"></a>`signature`: `"\"(self, request: 'ObjectReadRequest') -> 'ObjectReadStream'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](a-riverhog-filesystem-store-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-6533c1ed5b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-filesystem-store:a_riverhog_filesystem_store](../../../evidence/sources/authorities.md#src-d49395fbda) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_filesystem_store.FilesystemStorageAdapter.read_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fdf4f84dd48b84917e6ed4249b3f367f9daf448e6208f4421473ca7c39c8defb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ObjectReadRequest') -> 'ObjectReadStream'\""
  },
  "distribution": "a-riverhog-filesystem-store",
  "module": "a_riverhog_filesystem_store",
  "name": "read_object",
  "owner": "a_riverhog_filesystem_store.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
