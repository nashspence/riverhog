# a_riverhog_filesystem_store.FilesystemStorageAdapter.readiness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-filesystem-store:a-riverhog-filesystem-store-filesystemsto-04330693d5:62a407a30b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d6262cc210"></a>
- <a id="s-6651f8ffec"></a>`distribution`: `a-riverhog-filesystem-store`
- <a id="s-b3a89e8ace"></a>`module`: `a_riverhog_filesystem_store`
- <a id="s-22f24d2898"></a>`name`: `readiness`
- <a id="s-02a23119d8"></a>`owner`: `a_riverhog_filesystem_store.FilesystemStorageAdapter`
- <a id="s-7358b5c543"></a>`unit`: `member`

### Declared structure

- <a id="s-ce9277640c"></a>`kind`: `"method"`
- <a id="s-9bc9938e96"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](a-riverhog-filesystem-store-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-57a7e58675"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-filesystem-store:a_riverhog_filesystem_store](../../../evidence/sources/authorities.md#src-d49395fbda) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_filesystem_store.FilesystemStorageAdapter.readiness`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08c3fce4d0c46883b241dcfdda4d01bbc5028f12c0c784d86fd57b6e050eb0d8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "a-riverhog-filesystem-store",
  "module": "a_riverhog_filesystem_store",
  "name": "readiness",
  "owner": "a_riverhog_filesystem_store.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
