# time_formats.parse_duration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:time-formats:time-formats-parse-duration:4b9ab0b255 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-819df28ee6"></a>
- <a id="s-a0fd52646e"></a>`distribution`: `time-formats`
- <a id="s-1d29ee0035"></a>`module`: `time_formats`
- <a id="s-398929cf6a"></a>`name`: `parse_duration`
- <a id="s-9ddd52fea2"></a>`unit`: `export`

### Declared structure

- <a id="s-205739f30e"></a>`kind`: `"function"`
- <a id="s-409525981a"></a>`signature`: `"\"(value: 'str') -> 'timedelta'\""`

## Governing policies

- <a id="pa-473f0fbfdc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:time-formats:time_formats](../../../evidence/sources/authorities.md#src-4f4a8234ef) — [packages/time-formats/src/time\_formats/\_\_init\_\_.py](../../../../../../packages/time-formats/src/time_formats/__init__.py)

### Machine authority

- `/external_contract/python/time_formats.parse_duration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c9c280bf40eebc38567087f0d66b47817b4047623120b7b65603978be30bcb1d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'timedelta'\""
  },
  "distribution": "time-formats",
  "module": "time_formats",
  "name": "parse_duration",
  "unit": "export"
}
```

</details>
