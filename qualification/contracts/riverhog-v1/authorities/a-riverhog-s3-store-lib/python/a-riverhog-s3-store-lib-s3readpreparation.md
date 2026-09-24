# a_riverhog_s3_store_lib.S3ReadPreparation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3readpreparation:38a032e0bf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-127dd80c9f"></a>
- <a id="s-526867c530"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-6adb022eef"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-497c69b9ee"></a>`name`: `S3ReadPreparation`
- <a id="s-dd367c4535"></a>`unit`: `export`

### Declared structure

- <a id="s-e33ef9cac7"></a>`kind`: `"class"`
- <a id="s-eef9793703"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [cleanup](a-riverhog-s3-store-lib-s3readpreparation-cleanup.md)
- [prepare](a-riverhog-s3-store-lib-s3readpreparation-prepare.md)
- [status](a-riverhog-s3-store-lib-s3readpreparation-status.md)

## Governing policies

- <a id="pa-2094709de4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3ReadPreparation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e20b0723b829e4f918ea04ee859b922afce492b6ff42841f8911792c91a6d100 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "S3ReadPreparation",
  "unit": "export"
}
```

</details>
