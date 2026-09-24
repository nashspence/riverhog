# a_riverhog_s3_store_lib.S3StorageAdapter.cleanup_read

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3storageadapter-c6e3985a55:92559e45f7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ba7d1453bf"></a>
- <a id="s-24528a8f49"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-443b4c9990"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-c9f1c4629e"></a>`name`: `cleanup_read`
- <a id="s-0ec4a3181a"></a>`owner`: `a_riverhog_s3_store_lib.S3StorageAdapter`
- <a id="s-1f6a43c793"></a>`unit`: `member`

### Declared structure

- <a id="s-a9fb4b48a4"></a>`kind`: `"method"`
- <a id="s-4401b9aa3b"></a>`signature`: `"\"(self, request: 'ReadPreparationRequest') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [S3StorageAdapter](a-riverhog-s3-store-lib-s3storageadapter.md)

## Governing policies

- <a id="pa-8e7e5c8208"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3StorageAdapter.cleanup_read`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 080f1d6fa926f09a46131859ffdfc94d1f48aa0e4a42eda52802dfd45c749e96 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ReadPreparationRequest') -> 'None'\""
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "cleanup_read",
  "owner": "a_riverhog_s3_store_lib.S3StorageAdapter",
  "unit": "member"
}
```

</details>
