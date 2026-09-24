# a_riverhog_s3_store_lib.S3StorageAdapter.begin_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3storageadapter-begin-write:cb58ff52b0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d43664c053"></a>
- <a id="s-60c44c961f"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-32e2d936d5"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-ce103a357c"></a>`name`: `begin_write`
- <a id="s-c76ed697f0"></a>`owner`: `a_riverhog_s3_store_lib.S3StorageAdapter`
- <a id="s-ad7d2988d4"></a>`unit`: `member`

### Declared structure

- <a id="s-106a63bbf6"></a>`kind`: `"method"`
- <a id="s-57873d31f7"></a>`signature`: `"\"(self, request: 'WriteStartRequest') -> 'WriteSession'\""`

## Maintained corroboration

### Related interface records

- [S3StorageAdapter](a-riverhog-s3-store-lib-s3storageadapter.md)

## Governing policies

- <a id="pa-c8c8c4fd04"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3StorageAdapter.begin_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8007dcee5e6b62fcc30e91932da7049f34f519eee36abc6bc51f3b2a28c12c05 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteStartRequest') -> 'WriteSession'\""
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "begin_write",
  "owner": "a_riverhog_s3_store_lib.S3StorageAdapter",
  "unit": "member"
}
```

</details>
