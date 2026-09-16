# riverhog_storage_adapter_s3_support.S3StorageAdapter.read_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3sto-baf0ce4c7f:af1400d48b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e1833dc571"></a>
- <a id="s-3a600a3163"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-ae1485b3c6"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-0deee2604f"></a>`name`: `read_object`
- <a id="s-aea7f9af02"></a>`owner`: `riverhog_storage_adapter_s3_support.S3StorageAdapter`
- <a id="s-3d59af01ee"></a>`unit`: `member`

### Declared structure

- <a id="s-3d5ea233c2"></a>`kind`: `"method"`
- <a id="s-b03c0a5567"></a>`signature`: `"\"(self, request: 'ObjectReadRequest') -> 'ObjectReadStream'\""`

## Maintained corroboration

### Related interface records

- [S3StorageAdapter](riverhog-storage-adapter-s3-support-s3storageadapter.md)

## Governing policies

- <a id="pa-1388c1501c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources.md#src-aa14de5031) — `reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3StorageAdapter.read_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9efae6ca2b02fec28d5fc5b84abf5388f51fe637e40ef408d307759fd0973639 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ObjectReadRequest') -> 'ObjectReadStream'\""
  },
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "read_object",
  "owner": "riverhog_storage_adapter_s3_support.S3StorageAdapter",
  "unit": "member"
}
```

</details>
