# time_formats.CANONICAL_UTC_TIMESTAMP_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:time-formats:time-formats-canonical-utc-timestamp-pattern:d96e65d544 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fa4d3eeaea"></a>
- <a id="s-d10e6a0d7c"></a>`distribution`: `time-formats`
- <a id="s-67497f1df0"></a>`module`: `time_formats`
- <a id="s-fa4fb3bd80"></a>`name`: `CANONICAL_UTC_TIMESTAMP_PATTERN`
- <a id="s-2ee9b6bfb0"></a>`unit`: `export`

### Declared structure

- <a id="s-f047394423"></a>`kind`: `"constant"`
- <a id="s-11c5574ee6"></a>`value`: `"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"`

## Governing policies

- <a id="pa-68da1addb8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:time-formats:time_formats](../../../evidence/sources/authorities.md#src-4f4a8234ef) — [packages/time-formats/src/time\_formats/\_\_init\_\_.py](../../../../../../packages/time-formats/src/time_formats/__init__.py)

### Machine authority

- `/external_contract/python/time_formats.CANONICAL_UTC_TIMESTAMP_PATTERN`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e94eb5c99bd0a1234eee88e7b569c41aa13d27c2b32cc74e0388a48621d08ff1 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"
  },
  "distribution": "time-formats",
  "module": "time_formats",
  "name": "CANONICAL_UTC_TIMESTAMP_PATTERN",
  "unit": "export"
}
```

</details>
