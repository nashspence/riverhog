# time_formats.format_utc_ns

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:time-formats:time-formats-format-utc-ns:fbe64747c3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d0de48ead1"></a>
- <a id="s-52bf98e300"></a>`distribution`: `time-formats`
- <a id="s-aa7c0bc222"></a>`module`: `time_formats`
- <a id="s-89257b387a"></a>`name`: `format_utc_ns`
- <a id="s-4304380af8"></a>`unit`: `export`

### Declared structure

- <a id="s-bec79b3ea7"></a>`kind`: `"function"`
- <a id="s-4dd8fdbb79"></a>`signature`: `"\"(epoch_ns: 'int') -> 'str'\""`

## Governing policies

- <a id="pa-1520a6baa8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:time-formats:time_formats](../../../evidence/sources/authorities.md#src-4f4a8234ef) — [packages/time-formats/src/time\_formats/\_\_init\_\_.py](../../../../../../packages/time-formats/src/time_formats/__init__.py)

### Machine authority

- `/external_contract/python/time_formats.format_utc_ns`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c10c2c5bee3697cfb44228d53922d5756bc31f9e8090e6826a13728701208d1f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(epoch_ns: 'int') -> 'str'\""
  },
  "distribution": "time-formats",
  "module": "time_formats",
  "name": "format_utc_ns",
  "unit": "export"
}
```

</details>
