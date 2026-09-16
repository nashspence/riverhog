# riverhog_storage_adapter_s3_support.S3StorageAdapter.cleanup_read

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3sto-afcff0c8b6:ac6283ef9d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c48e87501f"></a>
- <a id="s-b8a19c16d2"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-ec8a0c96d2"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-c56111b798"></a>`name`: `cleanup_read`
- <a id="s-50581d13dd"></a>`owner`: `riverhog_storage_adapter_s3_support.S3StorageAdapter`
- <a id="s-dc18540849"></a>`unit`: `member`

### Declared structure

- <a id="s-1b1318e475"></a>`kind`: `"method"`
- <a id="s-c749dde5b9"></a>`signature`: `"\"(self, request: 'ReadPreparationRequest') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [S3StorageAdapter](riverhog-storage-adapter-s3-support-s3storageadapter.md)

## Governing policies

- <a id="pa-112a0ffe31"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources.md#src-aa14de5031) — `reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3StorageAdapter.cleanup_read`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 568dc414c403023f3a76af503c8a8c3ada304d8d5beb1f917378809ab6ce3ad0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ReadPreparationRequest') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "cleanup_read",
  "owner": "riverhog_storage_adapter_s3_support.S3StorageAdapter",
  "unit": "member"
}
```

</details>
