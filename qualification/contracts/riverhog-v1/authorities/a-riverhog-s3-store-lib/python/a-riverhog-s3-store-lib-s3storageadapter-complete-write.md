# a_riverhog_s3_store_lib.S3StorageAdapter.complete_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3storageadapter-c98aaf8126:c6c0a34c70 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f75d4ce7e0"></a>
- <a id="s-a30605c6fe"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-ca5003aabd"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-c4008810ac"></a>`name`: `complete_write`
- <a id="s-de09a49f9c"></a>`owner`: `a_riverhog_s3_store_lib.S3StorageAdapter`
- <a id="s-db28479ea3"></a>`unit`: `member`

### Declared structure

- <a id="s-1fb71a4d32"></a>`kind`: `"method"`
- <a id="s-d50033970a"></a>`signature`: `"\"(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'\""`

## Maintained corroboration

### Related interface records

- [S3StorageAdapter](a-riverhog-s3-store-lib-s3storageadapter.md)

## Governing policies

- <a id="pa-3f441d6ea5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3StorageAdapter.complete_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebd441a10adf84aba96aeb28f251d4db38b780f508cd0d3f3655697abbee99b2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'\""
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "complete_write",
  "owner": "a_riverhog_s3_store_lib.S3StorageAdapter",
  "unit": "member"
}
```

</details>
