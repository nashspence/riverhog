# time_formats.utc_timestamp_now

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:time-formats:time-formats-utc-timestamp-now:df0208a384 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ad7abe07f0"></a>
- <a id="s-6b7d2a3e40"></a>`distribution`: `time-formats`
- <a id="s-6d975e6dad"></a>`module`: `time_formats`
- <a id="s-a387ed783c"></a>`name`: `utc_timestamp_now`
- <a id="s-7a975aeac0"></a>`unit`: `export`

### Declared structure

- <a id="s-0b7f741a66"></a>`kind`: `"function"`
- <a id="s-43c8e13aec"></a>`signature`: `"\"() -> 'str'\""`

## Governing policies

- <a id="pa-2ab9f3e613"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:time-formats:time_formats](../../../evidence/sources/authorities.md#src-4f4a8234ef) — [packages/time-formats/src/time\_formats/\_\_init\_\_.py](../../../../../../packages/time-formats/src/time_formats/__init__.py)

### Machine authority

- `/external_contract/python/time_formats.utc_timestamp_now`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9dd519fc5db78a421778a811c10f805d7a8042bf21ee4ced7ba843cdbbd35784 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'str'\""
  },
  "distribution": "time-formats",
  "module": "time_formats",
  "name": "utc_timestamp_now",
  "unit": "export"
}
```

</details>
