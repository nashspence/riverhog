# a_riverhog_s3_store_lib.S3StorageAdapter.delete_prefix

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3storageadapter-b549a5eb15:fa19729847 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-69d2331b03"></a>
- <a id="s-8112906939"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-c755a185b4"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-02e10211b9"></a>`name`: `delete_prefix`
- <a id="s-1f41725396"></a>`owner`: `a_riverhog_s3_store_lib.S3StorageAdapter`
- <a id="s-fbf4947d6d"></a>`unit`: `member`

### Declared structure

- <a id="s-acf7d65b6f"></a>`kind`: `"method"`
- <a id="s-90448d5146"></a>`signature`: `"\"(self, request: 'DeletePrefixRequest') -> 'int'\""`

## Maintained corroboration

### Related interface records

- [S3StorageAdapter](a-riverhog-s3-store-lib-s3storageadapter.md)

## Governing policies

- <a id="pa-f6cdf9f018"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3StorageAdapter.delete_prefix`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f067e8294c77d12e78c9c7f16fe5baee1a2b63119656e451dd13c27e8cef41c2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'DeletePrefixRequest') -> 'int'\""
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "delete_prefix",
  "owner": "a_riverhog_s3_store_lib.S3StorageAdapter",
  "unit": "member"
}
```

</details>
