# review0_sampler_lib.SamplerWorkspace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-samplerworkspace:cfdc5b7d12 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0816012ec3"></a>
- <a id="s-f92f7ef242"></a>`distribution`: `review0-sampler-lib`
- <a id="s-6a03d1bd8b"></a>`module`: `review0_sampler_lib`
- <a id="s-c04aad8179"></a>`name`: `SamplerWorkspace`
- <a id="s-68da9c9a4e"></a>`unit`: `export`

### Declared structure

- <a id="s-857162b607"></a>`kind`: `"class"`
- <a id="s-a8c9ab2c54"></a>`signature`: `"\"(root: 'Path', request: 'SamplerRequest') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [canceled](review0-sampler-lib-samplerworkspace-canceled.md)
- [output](review0-sampler-lib-samplerworkspace-output.md)
- [resolve](review0-sampler-lib-samplerworkspace-resolve.md)
- [verify_input](review0-sampler-lib-samplerworkspace-verify-input.md)

## Governing policies

- <a id="pa-b69e230ae2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SamplerWorkspace`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2d8843257ced84619dc644794508d10f89f76b63c076b9eb939c4c2d2de94530 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(root: 'Path', request: 'SamplerRequest') -> 'None'\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "SamplerWorkspace",
  "unit": "export"
}
```

</details>
