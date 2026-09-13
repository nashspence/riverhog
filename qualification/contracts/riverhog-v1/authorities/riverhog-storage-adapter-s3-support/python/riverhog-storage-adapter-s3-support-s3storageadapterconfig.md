# riverhog_storage_adapter_s3_support.S3StorageAdapterConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3sto-839363913e:d7beb302dd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-409a9414cc"></a>
| Field | Shape |
|---|---|
| <a id="s-9c4ffb9031"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-6514d080eb"></a>`distribution` | "riverhog-storage-adapter-s3-support" |
| <a id="s-f343aab2a3"></a>`module` | "riverhog_storage_adapter_s3_support" |
| <a id="s-547121c938"></a>`name` | "S3StorageAdapterConfig" |
| <a id="s-6c0a9a89f2"></a>`unit` | "export" |

## Governing policies

- <a id="pa-202c1fdd32"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources.md#src-aa14de5031) — `reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3StorageAdapterConfig`

### Exact owned JSON

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
