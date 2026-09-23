# review0_target_lib.parse_sampler_registrations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-parse-sampler-registrations:e28d443ce7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e30ebd18bf"></a>
- <a id="s-a26db0c367"></a>`distribution`: `review0-target-lib`
- <a id="s-bb42a22eee"></a>`module`: `review0_target_lib`
- <a id="s-c2caf9bea4"></a>`name`: `parse_sampler_registrations`
- <a id="s-20ef2df58c"></a>`unit`: `export`

### Declared structure

- <a id="s-8197fabd2e"></a>`kind`: `"function"`
- <a id="s-eb3260270d"></a>`signature`: `"\"(document: 'str') -> 'tuple[SamplerRegistration, ...]'\""`

## Governing policies

- <a id="pa-cd60bc3e65"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.parse_sampler_registrations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a8caddee53cda02fe10b1d3400be2d8349609aae7c1426d2421677c3bbf7a37e -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(document: 'str') -> 'tuple[SamplerRegistration, ...]'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "parse_sampler_registrations",
  "unit": "export"
}
```

</details>
