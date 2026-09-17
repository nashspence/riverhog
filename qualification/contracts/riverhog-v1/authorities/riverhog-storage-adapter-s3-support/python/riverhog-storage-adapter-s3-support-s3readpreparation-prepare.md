# riverhog_storage_adapter_s3_support.S3ReadPreparation.prepare

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3rea-6dcb41c280:f0838be6dc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ad47d222f2"></a>
- <a id="s-1f3f394d67"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-108609ec65"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-05c6d5a0d9"></a>`name`: `prepare`
- <a id="s-4df1757ff9"></a>`owner`: `riverhog_storage_adapter_s3_support.S3ReadPreparation`
- <a id="s-643f1345d1"></a>`unit`: `member`

### Declared structure

- <a id="s-aef641284b"></a>`kind`: `"method"`
- <a id="s-abf4f5facb"></a>`signature`: `"\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str \| None], ...]') -> 'ReadReadiness'\""`

## Maintained corroboration

### Related interface records

- [S3ReadPreparation](riverhog-storage-adapter-s3-support-s3readpreparation.md)

## Governing policies

- <a id="pa-33a9f37f02"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources.md#src-aa14de5031) — [reference/riverhog/storage/s3-support/src/riverhog\_storage\_adapter\_s3\_support/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3ReadPreparation.prepare`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd79f0f3aac9f5ce9a551a9773268acc8e82ad06a4bce7e9cd694752f8462e5c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'ReadReadiness'\""
  },
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "prepare",
  "owner": "riverhog_storage_adapter_s3_support.S3ReadPreparation",
  "unit": "member"
}
```

</details>
