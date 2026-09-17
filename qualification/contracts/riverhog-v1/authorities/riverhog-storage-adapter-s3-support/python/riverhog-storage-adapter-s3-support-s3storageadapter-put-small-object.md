# riverhog_storage_adapter_s3_support.S3StorageAdapter.put_small_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3sto-fbebbe05c2:7090e3efe8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eb199953f9"></a>
- <a id="s-02477b8039"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-39b20aa3d1"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-c11a784b39"></a>`name`: `put_small_object`
- <a id="s-124d7ec275"></a>`owner`: `riverhog_storage_adapter_s3_support.S3StorageAdapter`
- <a id="s-9b01a472da"></a>`unit`: `member`

### Declared structure

- <a id="s-442b104167"></a>`kind`: `"method"`
- <a id="s-3f17e9755a"></a>`signature`: `"\"(self, request: 'SmallObjectWriteRequest', content: 'BinaryContent') -> 'ImmutableObjectReceipt'\""`

## Maintained corroboration

### Related interface records

- [S3StorageAdapter](riverhog-storage-adapter-s3-support-s3storageadapter.md)

## Governing policies

- <a id="pa-f0604529f1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources/authorities.md#src-aa14de5031) — [reference/riverhog/storage/s3-support/src/riverhog\_storage\_adapter\_s3\_support/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3StorageAdapter.put_small_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 46d9cffee84be06a1af003f0fda40612011f2a9618d0de75c8a18491b78a1241 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'SmallObjectWriteRequest', content: 'BinaryContent') -> 'ImmutableObjectReceipt'\""
  },
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "put_small_object",
  "owner": "riverhog_storage_adapter_s3_support.S3StorageAdapter",
  "unit": "member"
}
```

</details>
