# a_riverhog_filesystem_store.FilesystemStorageAdapter.write_segment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-filesystem-store:a-riverhog-filesystem-store-filesystemsto-5cb90a3900:bf9e8ae0da -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9be33105ad"></a>
- <a id="s-69fa437f89"></a>`distribution`: `a-riverhog-filesystem-store`
- <a id="s-8f51ab49ef"></a>`module`: `a_riverhog_filesystem_store`
- <a id="s-a0781ac498"></a>`name`: `write_segment`
- <a id="s-543051578b"></a>`owner`: `a_riverhog_filesystem_store.FilesystemStorageAdapter`
- <a id="s-0746da8053"></a>`unit`: `member`

### Declared structure

- <a id="s-5deb650675"></a>`kind`: `"method"`
- <a id="s-ac3f0ff112"></a>`signature`: `"\"(self, *, session: 'WriteSession', number: 'int', stored_bytes: 'int', content: 'BinaryContent') -> 'WriteSegmentReceipt'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](a-riverhog-filesystem-store-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-9aee2d97fe"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-filesystem-store:a_riverhog_filesystem_store](../../../evidence/sources/authorities.md#src-d49395fbda) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_filesystem_store.FilesystemStorageAdapter.write_segment`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 226b343d5e3944d538d34bf65a2d9eaee2f76e6a0d95fd49f353d2d7a1ad0967 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, session: 'WriteSession', number: 'int', stored_bytes: 'int', content: 'BinaryContent') -> 'WriteSegmentReceipt'\""
  },
  "distribution": "a-riverhog-filesystem-store",
  "module": "a_riverhog_filesystem_store",
  "name": "write_segment",
  "owner": "a_riverhog_filesystem_store.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
