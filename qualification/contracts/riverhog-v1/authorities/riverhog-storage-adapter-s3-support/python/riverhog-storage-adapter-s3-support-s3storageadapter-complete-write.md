# riverhog_storage_adapter_s3_support.S3StorageAdapter.complete_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3sto-2706c4c740:51dc0a1d1c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6563e984aa"></a>
- <a id="s-bb493ee713"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-ba1df68ceb"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-5c9725746a"></a>`name`: `complete_write`
- <a id="s-eb05f64af0"></a>`owner`: `riverhog_storage_adapter_s3_support.S3StorageAdapter`
- <a id="s-9b6295183f"></a>`unit`: `member`

### Declared structure

- <a id="s-e09cf269c5"></a>`kind`: `"method"`
- <a id="s-18251d80ff"></a>`signature`: `"\"(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'\""`

## Maintained corroboration

### Related interface records

- [S3StorageAdapter](riverhog-storage-adapter-s3-support-s3storageadapter.md)

## Governing policies

- <a id="pa-c645dd5cd0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources.md#src-aa14de5031) — [reference/riverhog/storage/s3-support/src/riverhog\_storage\_adapter\_s3\_support/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3StorageAdapter.complete_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e1dd0a10c68e318a2058a42f46738fa395acb9c32776fd97066172d8a53b433 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'\""
  },
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "complete_write",
  "owner": "riverhog_storage_adapter_s3_support.S3StorageAdapter",
  "unit": "member"
}
```

</details>
