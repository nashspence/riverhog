# riverhog_storage_adapter_s3_support.S3StorageAdapter.abort_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3sto-c73a1c56d5:136669ffd3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-82fd351986"></a>
- <a id="s-6a7a6cee50"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-b5338b57b0"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-c11cdaa9b8"></a>`name`: `abort_write`
- <a id="s-09b2a125f0"></a>`owner`: `riverhog_storage_adapter_s3_support.S3StorageAdapter`
- <a id="s-9b908e5c31"></a>`unit`: `member`

### Declared structure

- <a id="s-a5cb58fd05"></a>`kind`: `"method"`
- <a id="s-2ffba826da"></a>`signature`: `"\"(self, session: 'WriteSession') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [S3StorageAdapter](riverhog-storage-adapter-s3-support-s3storageadapter.md)

## Governing policies

- <a id="pa-03c17ef83f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources/authorities.md#src-aa14de5031) — [some-implementations/riverhog/storage/s3-support/src/riverhog\_storage\_adapter\_s3\_support/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3StorageAdapter.abort_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4a5fc74a8f608d93e8dc2319d3b87b987c067198da96ac116718b2fa6655054 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, session: 'WriteSession') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "abort_write",
  "owner": "riverhog_storage_adapter_s3_support.S3StorageAdapter",
  "unit": "member"
}
```

</details>
