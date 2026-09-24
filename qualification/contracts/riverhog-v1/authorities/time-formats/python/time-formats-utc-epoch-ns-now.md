# time_formats.utc_epoch_ns_now

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:time-formats:time-formats-utc-epoch-ns-now:a91240f235 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fca552a6ab"></a>
- <a id="s-133421c63a"></a>`distribution`: `time-formats`
- <a id="s-b359d7a001"></a>`module`: `time_formats`
- <a id="s-87bf9e71f0"></a>`name`: `utc_epoch_ns_now`
- <a id="s-a4d87036a0"></a>`unit`: `export`

### Declared structure

- <a id="s-f661c9fc02"></a>`kind`: `"function"`
- <a id="s-9e71239432"></a>`signature`: `"\"() -> 'int'\""`

## Governing policies

- <a id="pa-5ab276b237"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:time-formats:time_formats](../../../evidence/sources/authorities.md#src-4f4a8234ef) — [packages/time-formats/src/time\_formats/\_\_init\_\_.py](../../../../../../packages/time-formats/src/time_formats/__init__.py)

### Machine authority

- `/external_contract/python/time_formats.utc_epoch_ns_now`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 07bfc69eb5ea81092418fb577f060525371f88c40a9f1c5ed7500be0ac45eff0 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'int'\""
  },
  "distribution": "time-formats",
  "module": "time_formats",
  "name": "utc_epoch_ns_now",
  "unit": "export"
}
```

</details>
