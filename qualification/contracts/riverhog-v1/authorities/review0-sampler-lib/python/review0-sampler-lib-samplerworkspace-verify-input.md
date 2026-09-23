# review0_sampler_lib.SamplerWorkspace.verify_input

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-samplerworkspace-verify-input:76b74a588b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f07ca17ea8"></a>
- <a id="s-6734c9e198"></a>`distribution`: `review0-sampler-lib`
- <a id="s-4c7e3877bf"></a>`module`: `review0_sampler_lib`
- <a id="s-ebb3a53511"></a>`name`: `verify_input`
- <a id="s-69a8478d55"></a>`owner`: `review0_sampler_lib.SamplerWorkspace`
- <a id="s-b6e02e578b"></a>`unit`: `member`

### Declared structure

- <a id="s-2afdfbd597"></a>`kind`: `"method"`
- <a id="s-34280a023d"></a>`signature`: `"\"(self, declared: 'SamplerInput') -> 'Path'\""`

## Maintained corroboration

### Related interface records

- [SamplerWorkspace](review0-sampler-lib-samplerworkspace.md)

## Governing policies

- <a id="pa-9d9f3672cb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SamplerWorkspace.verify_input`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82bce658bfb903132ac904da15eb26e17268e3ec781a89b76bd51edd4f035c68 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, declared: 'SamplerInput') -> 'Path'\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "verify_input",
  "owner": "review0_sampler_lib.SamplerWorkspace",
  "unit": "member"
}
```

</details>
