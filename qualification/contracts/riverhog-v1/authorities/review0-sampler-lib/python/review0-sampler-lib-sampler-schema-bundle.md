# review0_sampler_lib.sampler_schema_bundle

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-sampler-schema-bundle:869f6735da -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a9d5dfa484"></a>
- <a id="s-65df89b8b1"></a>`distribution`: `review0-sampler-lib`
- <a id="s-4ed7851d20"></a>`module`: `review0_sampler_lib`
- <a id="s-cff22b7ae5"></a>`name`: `sampler_schema_bundle`
- <a id="s-23d0c3e458"></a>`unit`: `export`

### Declared structure

- <a id="s-53616a8f4b"></a>`kind`: `"function"`
- <a id="s-7547063ae3"></a>`signature`: `"\"() -> 'dict[str, Any]'\""`

## Governing policies

- <a id="pa-12fccdd255"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.sampler_schema_bundle`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e7d3ef4d39bb10d0c8beaa0a58f9c9f669e1ad31ba8bdebf9238ed8d058dc7c1 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'dict[str, Any]'\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "sampler_schema_bundle",
  "unit": "export"
}
```

</details>
