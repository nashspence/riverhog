# a_riverhog_s3_store_lib.S3ReadPreparation.prepare

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3readpreparation-prepare:f12f78a337 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-665c7c5c25"></a>
- <a id="s-de5744b477"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-374fcefab8"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-a0489d1fe7"></a>`name`: `prepare`
- <a id="s-41c61f445b"></a>`owner`: `a_riverhog_s3_store_lib.S3ReadPreparation`
- <a id="s-ea2bde9514"></a>`unit`: `member`

### Declared structure

- <a id="s-3b0f995fa9"></a>`kind`: `"method"`
- <a id="s-fadc712482"></a>`signature`: `"\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str \| None], ...]') -> 'ReadReadiness'\""`

## Maintained corroboration

### Related interface records

- [S3ReadPreparation](a-riverhog-s3-store-lib-s3readpreparation.md)

## Governing policies

- <a id="pa-48e22d886c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3ReadPreparation.prepare`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 933c9bccf7f0d699ffe48adf866005a0e4954e00ecbc881e409bd31cd66bd31a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'ReadReadiness'\""
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "prepare",
  "owner": "a_riverhog_s3_store_lib.S3ReadPreparation",
  "unit": "member"
}
```

</details>
