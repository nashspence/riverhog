# time_formats.parse_utc_timestamp

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:time-formats:time-formats-parse-utc-timestamp:6a7c26b0e4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8795590b7a"></a>
- <a id="s-93dd6b4f6f"></a>`distribution`: `time-formats`
- <a id="s-10ff2244c8"></a>`module`: `time_formats`
- <a id="s-c6927f57a7"></a>`name`: `parse_utc_timestamp`
- <a id="s-8db5b08acd"></a>`unit`: `export`

### Declared structure

- <a id="s-a06a3c94dc"></a>`kind`: `"function"`
- <a id="s-6b8eaf8bf4"></a>`signature`: `"\"(value: 'str') -> 'int'\""`

## Governing policies

- <a id="pa-d1a89623ee"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:time-formats:time_formats](../../../evidence/sources/authorities.md#src-4f4a8234ef) — [packages/time-formats/src/time\_formats/\_\_init\_\_.py](../../../../../../packages/time-formats/src/time_formats/__init__.py)

### Machine authority

- `/external_contract/python/time_formats.parse_utc_timestamp`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f328452e88deb29911f53d3882c4e712ce1b88b590ffdc0cf1c9829461cc4dc4 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'int'\""
  },
  "distribution": "time-formats",
  "module": "time_formats",
  "name": "parse_utc_timestamp",
  "unit": "export"
}
```

</details>
