# a_riverhog_filesystem_store.FilesystemStorageAdapter.list_segments

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-filesystem-store:a-riverhog-filesystem-store-filesystemsto-933931c8e9:252e375b7d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1697fd79e8"></a>
- <a id="s-bfff4d1816"></a>`distribution`: `a-riverhog-filesystem-store`
- <a id="s-33c70b545a"></a>`module`: `a_riverhog_filesystem_store`
- <a id="s-d53ae6537c"></a>`name`: `list_segments`
- <a id="s-176c3d420a"></a>`owner`: `a_riverhog_filesystem_store.FilesystemStorageAdapter`
- <a id="s-d7a524556f"></a>`unit`: `member`

### Declared structure

- <a id="s-78f38f7226"></a>`kind`: `"method"`
- <a id="s-de3e76f6b4"></a>`signature`: `"\"(self, request: 'WriteSegmentListRequest') -> 'WriteSegmentPage'\""`

## Maintained corroboration

### Related interface records

- [FilesystemStorageAdapter](a-riverhog-filesystem-store-filesystemstorageadapter.md)

## Governing policies

- <a id="pa-090c9b0fd3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-filesystem-store:a_riverhog_filesystem_store](../../../evidence/sources/authorities.md#src-d49395fbda) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_filesystem_store.FilesystemStorageAdapter.list_segments`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e6601bf9f251db52de45a5dc1ea2a0140109d1389f7960e7df260bb682efee4a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteSegmentListRequest') -> 'WriteSegmentPage'\""
  },
  "distribution": "a-riverhog-filesystem-store",
  "module": "a_riverhog_filesystem_store",
  "name": "list_segments",
  "owner": "a_riverhog_filesystem_store.FilesystemStorageAdapter",
  "unit": "member"
}
```

</details>
