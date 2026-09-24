# time_formats.datetime_from_utc_timestamp

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:time-formats:time-formats-datetime-from-utc-timestamp:306c116b55 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a836267300"></a>
- <a id="s-c079f8dec0"></a>`distribution`: `time-formats`
- <a id="s-719b964f93"></a>`module`: `time_formats`
- <a id="s-01b7782c69"></a>`name`: `datetime_from_utc_timestamp`
- <a id="s-b7d7f5b456"></a>`unit`: `export`

### Declared structure

- <a id="s-cccd7e9bed"></a>`kind`: `"function"`
- <a id="s-49fd6e226c"></a>`signature`: `"\"(value: 'str') -> 'datetime'\""`

## Governing policies

- <a id="pa-0de92ceca7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:time-formats:time_formats](../../../evidence/sources/authorities.md#src-4f4a8234ef) — [packages/time-formats/src/time\_formats/\_\_init\_\_.py](../../../../../../packages/time-formats/src/time_formats/__init__.py)

### Machine authority

- `/external_contract/python/time_formats.datetime_from_utc_timestamp`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e971ca7537720aeb295e6a18a61a70cca7b5f7249ddb70feb0185817855ba146 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'datetime'\""
  },
  "distribution": "time-formats",
  "module": "time_formats",
  "name": "datetime_from_utc_timestamp",
  "unit": "export"
}
```

</details>
