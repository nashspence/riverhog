# a_riverhog_filesystem_store.FilesystemStorageAdapterConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-filesystem-store:a-riverhog-filesystem-store-filesystemsto-92ea432e4b:e2ee8ac648 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9a2f215b29"></a>
- <a id="s-77dc3050f3"></a>`distribution`: `a-riverhog-filesystem-store`
- <a id="s-0521e2b1a4"></a>`module`: `a_riverhog_filesystem_store`
- <a id="s-2b3e313682"></a>`name`: `FilesystemStorageAdapterConfig`
- <a id="s-14494d1e54"></a>`unit`: `export`

### Declared structure

- <a id="s-5177f6ca48"></a>`kind`: `"class"`
- <a id="s-ed94b221b1"></a>`signature`: `"\"(root: 'Path', implementation_id: 'str' = 'a-riverhog-filesystem-store/v1', implementation_version: 'str' = '0.1.0', segment_bytes: 'int' = 67108864, read_chunk_bytes: 'int' = 8388608, minimum_free_bytes: 'int' = 268435456) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-f1747e5986"></a>`root` | `'Path'` | `required` |
| <a id="s-ec0f7fba7f"></a>`implementation_id` | `'str'` | `'a-riverhog-filesystem-store/v1'` |
| <a id="s-76fd8a1fba"></a>`implementation_version` | `'str'` | `'0.1.0'` |
| <a id="s-097843f2aa"></a>`segment_bytes` | `'int'` | `67108864` |
| <a id="s-999a680120"></a>`read_chunk_bytes` | `'int'` | `8388608` |
| <a id="s-5830506201"></a>`minimum_free_bytes` | `'int'` | `268435456` |

## Governing policies

- <a id="pa-57fc7885f6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-filesystem-store:a_riverhog_filesystem_store](../../../evidence/sources/authorities.md#src-d49395fbda) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_filesystem_store.FilesystemStorageAdapterConfig`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e416695fda2fb87979b4854034ae3401885df7b1a0db02a8bc7a98c2337540f -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "root",
        "type": "'Path'"
      },
      {
        "default": "'a-riverhog-filesystem-store/v1'",
        "name": "implementation_id",
        "type": "'str'"
      },
      {
        "default": "'0.1.0'",
        "name": "implementation_version",
        "type": "'str'"
      },
      {
        "default": "67108864",
        "name": "segment_bytes",
        "type": "'int'"
      },
      {
        "default": "8388608",
        "name": "read_chunk_bytes",
        "type": "'int'"
      },
      {
        "default": "268435456",
        "name": "minimum_free_bytes",
        "type": "'int'"
      }
    ],
    "kind": "class",
    "signature": "\"(root: 'Path', implementation_id: 'str' = 'a-riverhog-filesystem-store/v1', implementation_version: 'str' = '0.1.0', segment_bytes: 'int' = 67108864, read_chunk_bytes: 'int' = 8388608, minimum_free_bytes: 'int' = 268435456) -> None\""
  },
  "distribution": "a-riverhog-filesystem-store",
  "module": "a_riverhog_filesystem_store",
  "name": "FilesystemStorageAdapterConfig",
  "unit": "export"
}
```

</details>
