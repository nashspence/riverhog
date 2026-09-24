# a_riverhog_s3_store_lib.S3StorageAdapter.list_segments

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3storageadapter-845d60ae6b:d0e6b09552 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1f9d76e63c"></a>
- <a id="s-dabfb482b6"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-df0a4b85c3"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-d252ffaca9"></a>`name`: `list_segments`
- <a id="s-42bd18c4c8"></a>`owner`: `a_riverhog_s3_store_lib.S3StorageAdapter`
- <a id="s-dd17cb3944"></a>`unit`: `member`

### Declared structure

- <a id="s-8ad5a97f61"></a>`kind`: `"method"`
- <a id="s-f87a13b77f"></a>`signature`: `"\"(self, request: 'WriteSegmentListRequest') -> 'WriteSegmentPage'\""`

## Maintained corroboration

### Related interface records

- [S3StorageAdapter](a-riverhog-s3-store-lib-s3storageadapter.md)

## Governing policies

- <a id="pa-602e4af3e4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3StorageAdapter.list_segments`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4b3d55f9f175c5c18307e85d2c5dff90c0dda09138f920b66f0bff2cd78549f8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteSegmentListRequest') -> 'WriteSegmentPage'\""
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "list_segments",
  "owner": "a_riverhog_s3_store_lib.S3StorageAdapter",
  "unit": "member"
}
```

</details>
