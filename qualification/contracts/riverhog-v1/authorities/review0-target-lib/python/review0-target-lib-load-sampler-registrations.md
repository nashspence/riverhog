# review0_target_lib.load_sampler_registrations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-load-sampler-registrations:2963db8b62 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-909ea30132"></a>
- <a id="s-1cf447bfd3"></a>`distribution`: `review0-target-lib`
- <a id="s-a0e38e8b32"></a>`module`: `review0_target_lib`
- <a id="s-cfce2b4895"></a>`name`: `load_sampler_registrations`
- <a id="s-f683decca4"></a>`unit`: `export`

### Declared structure

- <a id="s-59d4186365"></a>`kind`: `"function"`
- <a id="s-eb48b8519b"></a>`signature`: `"\"(path: 'Path') -> 'tuple[SamplerRegistration, ...]'\""`

## Governing policies

- <a id="pa-e53a8cb1ce"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.load_sampler_registrations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f79fa98da3a74a8a40299031730fe1626a7234d472f44209c6dc59f8a52438a6 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path') -> 'tuple[SamplerRegistration, ...]'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "load_sampler_registrations",
  "unit": "export"
}
```

</details>
