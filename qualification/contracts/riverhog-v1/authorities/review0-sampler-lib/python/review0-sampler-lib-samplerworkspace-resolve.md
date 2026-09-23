# review0_sampler_lib.SamplerWorkspace.resolve

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-samplerworkspace-resolve:8bbbf5e69d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a56359f05b"></a>
- <a id="s-897a2fe107"></a>`distribution`: `review0-sampler-lib`
- <a id="s-d47340e30e"></a>`module`: `review0_sampler_lib`
- <a id="s-abfb6a2f90"></a>`name`: `resolve`
- <a id="s-ed589f1d27"></a>`owner`: `review0_sampler_lib.SamplerWorkspace`
- <a id="s-8937958639"></a>`unit`: `member`

### Declared structure

- <a id="s-3c470c73ed"></a>`kind`: `"method"`
- <a id="s-f7ee1a0864"></a>`signature`: `"\"(self, relative_path: 'str') -> 'Path'\""`

## Maintained corroboration

### Related interface records

- [SamplerWorkspace](review0-sampler-lib-samplerworkspace.md)

## Governing policies

- <a id="pa-f84f15c02f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SamplerWorkspace.resolve`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aca0ba1871499c43dfb9a0eb56482f0ebb172aad2f2db8577977ca89b0a4459c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, relative_path: 'str') -> 'Path'\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "resolve",
  "owner": "review0_sampler_lib.SamplerWorkspace",
  "unit": "member"
}
```

</details>
