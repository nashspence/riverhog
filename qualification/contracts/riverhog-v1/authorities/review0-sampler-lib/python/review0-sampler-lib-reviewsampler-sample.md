# review0_sampler_lib.ReviewSampler.sample

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-reviewsampler-sample:51b07c514a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ae65e95908"></a>
- <a id="s-65dbbbfcf3"></a>`distribution`: `review0-sampler-lib`
- <a id="s-2befec9bb1"></a>`module`: `review0_sampler_lib`
- <a id="s-527ab74c79"></a>`name`: `sample`
- <a id="s-17ddd49dd2"></a>`owner`: `review0_sampler_lib.ReviewSampler`
- <a id="s-4d23d467c0"></a>`unit`: `member`

### Declared structure

- <a id="s-dd876d0cac"></a>`kind`: `"method"`
- <a id="s-d2b8e71e60"></a>`signature`: `"\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""`

## Maintained corroboration

### Related interface records

- [ReviewSampler](review0-sampler-lib-reviewsampler.md)

## Governing policies

- <a id="pa-4bb6341ecb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.ReviewSampler.sample`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0bc1e816687d22727f4c816a3b4667dfd135edc51502385707891c89934143d0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "sample",
  "owner": "review0_sampler_lib.ReviewSampler",
  "unit": "member"
}
```

</details>
