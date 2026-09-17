# riverhog_storage_adapter_s3_support.S3ReadPreparation.cleanup

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3rea-e67a54535a:89db4ce034 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-214d8ed7f8"></a>
- <a id="s-473e573d10"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-ad06e70670"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-0236f8cd4f"></a>`name`: `cleanup`
- <a id="s-23ab1ae07f"></a>`owner`: `riverhog_storage_adapter_s3_support.S3ReadPreparation`
- <a id="s-3215f3316e"></a>`unit`: `member`

### Declared structure

- <a id="s-31ac79ad0d"></a>`kind`: `"method"`
- <a id="s-776c38b39f"></a>`signature`: `"\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str \| None], ...]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [S3ReadPreparation](riverhog-storage-adapter-s3-support-s3readpreparation.md)

## Governing policies

- <a id="pa-66fdfaec0d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources/authorities.md#src-aa14de5031) — [reference/riverhog/storage/s3-support/src/riverhog\_storage\_adapter\_s3\_support/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3ReadPreparation.cleanup`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59d0780f5b7419c044c5c2e5d26490ce539113de7b17744a568d10c258e17a73 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "cleanup",
  "owner": "riverhog_storage_adapter_s3_support.S3ReadPreparation",
  "unit": "member"
}
```

</details>
