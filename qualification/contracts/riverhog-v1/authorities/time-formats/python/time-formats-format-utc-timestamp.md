# time_formats.format_utc_timestamp

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:time-formats:time-formats-format-utc-timestamp:bb520b7c23 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f8d2ae4d12"></a>
- <a id="s-34fedf684f"></a>`distribution`: `time-formats`
- <a id="s-f9f3e4402b"></a>`module`: `time_formats`
- <a id="s-3f4b71a452"></a>`name`: `format_utc_timestamp`
- <a id="s-5bc85cd20a"></a>`unit`: `export`

### Declared structure

- <a id="s-f1ad2d81a5"></a>`kind`: `"function"`
- <a id="s-fa5b250b90"></a>`signature`: `"\"(value: 'datetime') -> 'str'\""`

## Governing policies

- <a id="pa-c3e9b01dbc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:time-formats:time_formats](../../../evidence/sources/authorities.md#src-4f4a8234ef) — [packages/time-formats/src/time\_formats/\_\_init\_\_.py](../../../../../../packages/time-formats/src/time_formats/__init__.py)

### Machine authority

- `/external_contract/python/time_formats.format_utc_timestamp`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc5ebc6f6bc6943787544a86d3b4058d33f2472ace7ae55210eb071952b91a2c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'datetime') -> 'str'\""
  },
  "distribution": "time-formats",
  "module": "time_formats",
  "name": "format_utc_timestamp",
  "unit": "export"
}
```

</details>
