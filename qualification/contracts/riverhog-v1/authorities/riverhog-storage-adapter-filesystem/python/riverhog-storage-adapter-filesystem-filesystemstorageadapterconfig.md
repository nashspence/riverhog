# riverhog_storage_adapter_filesystem.FilesystemStorageAdapterConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-files-694892e139:e090ea5f26 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-24685130e9"></a>
- <a id="s-77ec06b05b"></a>`distribution`: `riverhog-storage-adapter-filesystem`
- <a id="s-d4acde7d9c"></a>`module`: `riverhog_storage_adapter_filesystem`
- <a id="s-702600b7c8"></a>`name`: `FilesystemStorageAdapterConfig`
- <a id="s-8f2245110e"></a>`unit`: `export`

### Declared structure

- <a id="s-f994fb64a6"></a>`kind`: `"class"`
- <a id="s-9374821503"></a>`signature`: `"\"(root: 'Path', implementation_id: 'str' = 'riverhog.filesystem/v1', implementation_version: 'str' = '0.1.0', segment_bytes: 'int' = 67108864, read_chunk_bytes: 'int' = 8388608, minimum_free_bytes: 'int' = 268435456) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-ba243b10d5"></a>`root` | `'Path'` | `required` |
| <a id="s-f4d3819d2d"></a>`implementation_id` | `'str'` | `'riverhog.filesystem/v1'` |
| <a id="s-877eb9afd4"></a>`implementation_version` | `'str'` | `'0.1.0'` |
| <a id="s-e76ee9a28f"></a>`segment_bytes` | `'int'` | `67108864` |
| <a id="s-33b1f6b07b"></a>`read_chunk_bytes` | `'int'` | `8388608` |
| <a id="s-1d62bf467e"></a>`minimum_free_bytes` | `'int'` | `268435456` |

## Governing policies

- <a id="pa-c6c4f0cd41"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../../../evidence/sources.md#src-e075952170) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_filesystem.FilesystemStorageAdapterConfig`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb389baecb5a75e3f801a20632b352d2338ba895f4e2a8667c7cbd71e264bbfe -->

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
        "default": "'riverhog.filesystem/v1'",
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
    "signature": "\"(root: 'Path', implementation_id: 'str' = 'riverhog.filesystem/v1', implementation_version: 'str' = '0.1.0', segment_bytes: 'int' = 67108864, read_chunk_bytes: 'int' = 8388608, minimum_free_bytes: 'int' = 268435456) -> None\""
  },
  "distribution": "riverhog-storage-adapter-filesystem",
  "module": "riverhog_storage_adapter_filesystem",
  "name": "FilesystemStorageAdapterConfig",
  "unit": "export"
}
```

</details>
