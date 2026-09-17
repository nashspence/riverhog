# riverhog_storage_adapter_s3_support.S3ReadPreparation.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3rea-b5cbf9f070:7ab2e153bf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a719092c4c"></a>
- <a id="s-5ccaef2610"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-5433515c03"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-ac4e57555b"></a>`name`: `status`
- <a id="s-11e0bc9641"></a>`owner`: `riverhog_storage_adapter_s3_support.S3ReadPreparation`
- <a id="s-4067d22aac"></a>`unit`: `member`

### Declared structure

- <a id="s-9f0c68816d"></a>`kind`: `"method"`
- <a id="s-2723a1907f"></a>`signature`: `"\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str \| None], ...]') -> 'ReadReadiness'\""`

## Maintained corroboration

### Related interface records

- [S3ReadPreparation](riverhog-storage-adapter-s3-support-s3readpreparation.md)

## Governing policies

- <a id="pa-edd693824a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources.md#src-aa14de5031) — [reference/riverhog/storage/s3-support/src/riverhog\_storage\_adapter\_s3\_support/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3ReadPreparation.status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: adfe6682923ba4008428b914e6cba73d1e6a3a3420502c084accdb72efdba1b4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'ReadReadiness'\""
  },
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "status",
  "owner": "riverhog_storage_adapter_s3_support.S3ReadPreparation",
  "unit": "member"
}
```

</details>
