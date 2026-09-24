# a_riverhog_s3_store_lib.S3StorageAdapter.delete_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3storageadapter-4bed666a3f:45a4799130 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-95c21303e9"></a>
- <a id="s-cfe010ec3b"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-3b541b6f2d"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-887b622efc"></a>`name`: `delete_object`
- <a id="s-567400b5b0"></a>`owner`: `a_riverhog_s3_store_lib.S3StorageAdapter`
- <a id="s-67c24f4539"></a>`unit`: `member`

### Declared structure

- <a id="s-2d6300635d"></a>`kind`: `"method"`
- <a id="s-5760429487"></a>`signature`: `"\"(self, request: 'DeleteObjectRequest') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [S3StorageAdapter](a-riverhog-s3-store-lib-s3storageadapter.md)

## Governing policies

- <a id="pa-bf2c5c5043"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3StorageAdapter.delete_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e988e060c17e5a159c1405a3c7cc5989ae9b4955b7f1b36415bf6fbe768d6101 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'DeleteObjectRequest') -> 'None'\""
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "delete_object",
  "owner": "a_riverhog_s3_store_lib.S3StorageAdapter",
  "unit": "member"
}
```

</details>
