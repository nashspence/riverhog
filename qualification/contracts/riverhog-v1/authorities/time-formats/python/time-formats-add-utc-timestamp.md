# time_formats.add_utc_timestamp

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:time-formats:time-formats-add-utc-timestamp:a7384c178d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9322c1162a"></a>
- <a id="s-57015f61ef"></a>`distribution`: `time-formats`
- <a id="s-22a128e1a8"></a>`module`: `time_formats`
- <a id="s-ef5d49e980"></a>`name`: `add_utc_timestamp`
- <a id="s-4598b989de"></a>`unit`: `export`

### Declared structure

- <a id="s-7b70697526"></a>`kind`: `"function"`
- <a id="s-6e5186be95"></a>`signature`: `"\"(value: 'str', duration: 'timedelta') -> 'str'\""`

## Governing policies

- <a id="pa-bad566a462"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:time-formats:time_formats](../../../evidence/sources/authorities.md#src-4f4a8234ef) — [packages/time-formats/src/time\_formats/\_\_init\_\_.py](../../../../../../packages/time-formats/src/time_formats/__init__.py)

### Machine authority

- `/external_contract/python/time_formats.add_utc_timestamp`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 15e007594737a07ef4e991458bbf28adb24cd9597db0277a204d41d63cf09e46 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str', duration: 'timedelta') -> 'str'\""
  },
  "distribution": "time-formats",
  "module": "time_formats",
  "name": "add_utc_timestamp",
  "unit": "export"
}
```

</details>
