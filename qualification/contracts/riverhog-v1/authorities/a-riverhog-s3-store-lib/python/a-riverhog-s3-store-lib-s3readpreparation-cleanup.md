# a_riverhog_s3_store_lib.S3ReadPreparation.cleanup

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3readpreparation-cleanup:8e24285b62 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d93a3c8138"></a>
- <a id="s-918b8a5ea7"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-7b29a9c450"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-2cd7746b15"></a>`name`: `cleanup`
- <a id="s-04bde2da8b"></a>`owner`: `a_riverhog_s3_store_lib.S3ReadPreparation`
- <a id="s-627bda60e5"></a>`unit`: `member`

### Declared structure

- <a id="s-4ff460bce4"></a>`kind`: `"method"`
- <a id="s-bbf646ced8"></a>`signature`: `"\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str \| None], ...]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [S3ReadPreparation](a-riverhog-s3-store-lib-s3readpreparation.md)

## Governing policies

- <a id="pa-b36edef306"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3ReadPreparation.cleanup`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d35eee089c59a07eda0991cb12c7afdc99276c34ba9a2c165f00268205101e6f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'None'\""
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "cleanup",
  "owner": "a_riverhog_s3_store_lib.S3ReadPreparation",
  "unit": "member"
}
```

</details>
