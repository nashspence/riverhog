# riverhog_storage_adapter_s3_support.S3StorageAdapter.delete_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3sto-7900389d86:3ce9c7bc4f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f65b9ddd7e"></a>
- <a id="s-9cf684df66"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-e016fc16c4"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-898fe3dd17"></a>`name`: `delete_object`
- <a id="s-f80a471669"></a>`owner`: `riverhog_storage_adapter_s3_support.S3StorageAdapter`
- <a id="s-311a3d56f1"></a>`unit`: `member`

### Declared structure

- <a id="s-5db28e5868"></a>`kind`: `"method"`
- <a id="s-954df560d5"></a>`signature`: `"\"(self, request: 'DeleteObjectRequest') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [S3StorageAdapter](riverhog-storage-adapter-s3-support-s3storageadapter.md)

## Governing policies

- <a id="pa-68e0e58e03"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources.md#src-aa14de5031) — [reference/riverhog/storage/s3-support/src/riverhog\_storage\_adapter\_s3\_support/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3StorageAdapter.delete_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4059b8385ba0fce45d3b8eb8ad07c325cd638d9db47727d7dc2162db7730ec89 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'DeleteObjectRequest') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "delete_object",
  "owner": "riverhog_storage_adapter_s3_support.S3StorageAdapter",
  "unit": "member"
}
```

</details>
