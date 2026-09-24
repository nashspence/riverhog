# time_formats.epoch_ns_from_datetime

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:time-formats:time-formats-epoch-ns-from-datetime:b421a6e46f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-85db016085"></a>
- <a id="s-350461f224"></a>`distribution`: `time-formats`
- <a id="s-a2e618252c"></a>`module`: `time_formats`
- <a id="s-42f2e354b6"></a>`name`: `epoch_ns_from_datetime`
- <a id="s-4bddbf73f6"></a>`unit`: `export`

### Declared structure

- <a id="s-e6d3878d76"></a>`kind`: `"function"`
- <a id="s-7bb77692d6"></a>`signature`: `"\"(value: 'datetime') -> 'int'\""`

## Governing policies

- <a id="pa-9d14cdca89"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:time-formats:time_formats](../../../evidence/sources/authorities.md#src-4f4a8234ef) — [packages/time-formats/src/time\_formats/\_\_init\_\_.py](../../../../../../packages/time-formats/src/time_formats/__init__.py)

### Machine authority

- `/external_contract/python/time_formats.epoch_ns_from_datetime`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bec568e6a35afafff17f49f90149eeeeeb926bb47f48aaea32cb5fa8c2c816a3 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'datetime') -> 'int'\""
  },
  "distribution": "time-formats",
  "module": "time_formats",
  "name": "epoch_ns_from_datetime",
  "unit": "export"
}
```

</details>
