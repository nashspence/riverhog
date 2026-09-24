# a_riverhog_s3_store_lib.S3StorageAdapterConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3storageadapterconfig:3c5f5eeb3c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6cf6e38e83"></a>
- <a id="s-90c1bb76f3"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-5f44d2837b"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-99f81eb368"></a>`name`: `S3StorageAdapterConfig`
- <a id="s-53b1e73f49"></a>`unit`: `export`

### Declared structure

- <a id="s-12af9b6a88"></a>`kind`: `"class"`
- <a id="s-b05e7b3b57"></a>`signature`: `"\"(implementation_id: 'str', implementation_version: 'str', bucket: 'str', root_prefix: 'str' = '', read_mode: 'ReadMode' = 'immediate', archive_storage_class: 'str \| None' = None, immediate_storage_class: 'str \| None' = None, read_chunk_bytes: 'int' = 8388608) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-7a1d70b5a6"></a>`implementation_id` | `'str'` | `required` |
| <a id="s-81672e9e98"></a>`implementation_version` | `'str'` | `required` |
| <a id="s-7288468f57"></a>`bucket` | `'str'` | `required` |
| <a id="s-a459da2a9b"></a>`root_prefix` | `'str'` | `''` |
| <a id="s-19398ff777"></a>`read_mode` | `'ReadMode'` | `'immediate'` |
| <a id="s-ffe5309a52"></a>`archive_storage_class` | `'str \| None'` | `None` |
| <a id="s-5020952453"></a>`immediate_storage_class` | `'str \| None'` | `None` |
| <a id="s-09c8f27c83"></a>`read_chunk_bytes` | `'int'` | `8388608` |

## Governing policies

- <a id="pa-92dc00b920"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3StorageAdapterConfig`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8362370c85475633bd33a18f922a8215dd30ca01c6b4cf4edaf156b2d21bbe33 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "implementation_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "implementation_version",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "bucket",
        "type": "'str'"
      },
      {
        "default": "''",
        "name": "root_prefix",
        "type": "'str'"
      },
      {
        "default": "'immediate'",
        "name": "read_mode",
        "type": "'ReadMode'"
      },
      {
        "default": "None",
        "name": "archive_storage_class",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "immediate_storage_class",
        "type": "'str | None'"
      },
      {
        "default": "8388608",
        "name": "read_chunk_bytes",
        "type": "'int'"
      }
    ],
    "kind": "class",
    "signature": "\"(implementation_id: 'str', implementation_version: 'str', bucket: 'str', root_prefix: 'str' = '', read_mode: 'ReadMode' = 'immediate', archive_storage_class: 'str | None' = None, immediate_storage_class: 'str | None' = None, read_chunk_bytes: 'int' = 8388608) -> None\""
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "S3StorageAdapterConfig",
  "unit": "export"
}
```

</details>
