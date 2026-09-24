# time_formats.normalize_utc_timestamp

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:time-formats:time-formats-normalize-utc-timestamp:0e9c20883a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-59c896aec8"></a>
- <a id="s-9b1a822ec6"></a>`distribution`: `time-formats`
- <a id="s-48257b3515"></a>`module`: `time_formats`
- <a id="s-84439b54c0"></a>`name`: `normalize_utc_timestamp`
- <a id="s-9cfe09c233"></a>`unit`: `export`

### Declared structure

- <a id="s-20d4bc4c69"></a>`kind`: `"function"`
- <a id="s-be5e1821b3"></a>`signature`: `"\"(value: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-aa118360ab"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:time-formats:time_formats](../../../evidence/sources/authorities.md#src-4f4a8234ef) — [packages/time-formats/src/time\_formats/\_\_init\_\_.py](../../../../../../packages/time-formats/src/time_formats/__init__.py)

### Machine authority

- `/external_contract/python/time_formats.normalize_utc_timestamp`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3f66e8a23b754fd3ba2cce1262aca7a81581c294c7d6b31ab2c7ec56b6088f78 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'str'\""
  },
  "distribution": "time-formats",
  "module": "time_formats",
  "name": "normalize_utc_timestamp",
  "unit": "export"
}
```

</details>
