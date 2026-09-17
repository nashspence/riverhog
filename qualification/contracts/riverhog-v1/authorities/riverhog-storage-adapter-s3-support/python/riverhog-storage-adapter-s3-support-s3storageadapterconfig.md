# riverhog_storage_adapter_s3_support.S3StorageAdapterConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3sto-839363913e:d7beb302dd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-409a9414cc"></a>
- <a id="s-6514d080eb"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-f343aab2a3"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-547121c938"></a>`name`: `S3StorageAdapterConfig`
- <a id="s-6c0a9a89f2"></a>`unit`: `export`

### Declared structure

- <a id="s-0362c5cbd3"></a>`kind`: `"class"`
- <a id="s-02e980e7ec"></a>`signature`: `"\"(implementation_id: 'str', implementation_version: 'str', bucket: 'str', root_prefix: 'str' = '', read_mode: 'ReadMode' = 'immediate', archive_storage_class: 'str \| None' = None, immediate_storage_class: 'str \| None' = None, read_chunk_bytes: 'int' = 8388608) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-558c0673b3"></a>`implementation_id` | `'str'` | `required` |
| <a id="s-62239367d9"></a>`implementation_version` | `'str'` | `required` |
| <a id="s-d1c2e9b5b9"></a>`bucket` | `'str'` | `required` |
| <a id="s-68853aeb54"></a>`root_prefix` | `'str'` | `''` |
| <a id="s-23d31ff1ab"></a>`read_mode` | `'ReadMode'` | `'immediate'` |
| <a id="s-42a248ba5b"></a>`archive_storage_class` | `'str \| None'` | `None` |
| <a id="s-f7f1196297"></a>`immediate_storage_class` | `'str \| None'` | `None` |
| <a id="s-c88970db05"></a>`read_chunk_bytes` | `'int'` | `8388608` |

## Governing policies

- <a id="pa-202c1fdd32"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources/authorities.md#src-aa14de5031) — [reference/riverhog/storage/s3-support/src/riverhog\_storage\_adapter\_s3\_support/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3StorageAdapterConfig`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d62219f25aea686c26e153478ecbb139d42f20dd88d16001c27d3a5eb1b7a581 -->

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
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "S3StorageAdapterConfig",
  "unit": "export"
}
```

</details>
