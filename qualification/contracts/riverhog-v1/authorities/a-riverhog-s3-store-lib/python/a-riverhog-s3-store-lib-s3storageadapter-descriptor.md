# a_riverhog_s3_store_lib.S3StorageAdapter.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3storageadapter-descriptor:aed74284d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-736d331409"></a>
- <a id="s-77e8c2e13f"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-1d2f0f54d8"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-ea2b69e422"></a>`name`: `descriptor`
- <a id="s-5396fd0630"></a>`owner`: `a_riverhog_s3_store_lib.S3StorageAdapter`
- <a id="s-70c2fa9bf6"></a>`unit`: `member`

### Declared structure

- <a id="s-0827f868e1"></a>`kind`: `"method"`
- <a id="s-213a747dd1"></a>`signature`: `"\"(self) -> 'AdapterDescriptor'\""`

## Maintained corroboration

### Related interface records

- [S3StorageAdapter](a-riverhog-s3-store-lib-s3storageadapter.md)

## Governing policies

- <a id="pa-bd4b29e6ae"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3StorageAdapter.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 456633f354932d81514ef9c79331deddce207ae3d54385552df7dada47ce1d78 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'AdapterDescriptor'\""
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "descriptor",
  "owner": "a_riverhog_s3_store_lib.S3StorageAdapter",
  "unit": "member"
}
```

</details>
