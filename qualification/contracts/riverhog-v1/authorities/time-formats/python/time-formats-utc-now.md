# time_formats.utc_now

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:time-formats:time-formats-utc-now:44127efbd7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9cb40f5a28"></a>
- <a id="s-d7c6783dc5"></a>`distribution`: `time-formats`
- <a id="s-044520e68a"></a>`module`: `time_formats`
- <a id="s-3023676136"></a>`name`: `utc_now`
- <a id="s-4d1f162d23"></a>`unit`: `export`

### Declared structure

- <a id="s-4a15217f2b"></a>`kind`: `"function"`
- <a id="s-064adc7f2f"></a>`signature`: `"\"() -> 'datetime'\""`

## Governing policies

- <a id="pa-4fe6b4be1d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:time-formats:time_formats](../../../evidence/sources/authorities.md#src-4f4a8234ef) — [packages/time-formats/src/time\_formats/\_\_init\_\_.py](../../../../../../packages/time-formats/src/time_formats/__init__.py)

### Machine authority

- `/external_contract/python/time_formats.utc_now`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 36ff0bca83e61194d622dd071d9b515ef64f6791ce73f2dbefd330e1d38a9c99 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'datetime'\""
  },
  "distribution": "time-formats",
  "module": "time_formats",
  "name": "utc_now",
  "unit": "export"
}
```

</details>
