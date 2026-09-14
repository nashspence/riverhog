# stove0_review_sampler_support.SamplerClient.sample

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-samplerclient-sample:f8043887ea -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5c7ada85fb"></a>
- <a id="s-04f3298d35"></a>`distribution`: `stove0-review-sampler-support`
- <a id="s-c8280093b6"></a>`module`: `stove0_review_sampler_support`
- <a id="s-0b7ccae833"></a>`name`: `sample`
- <a id="s-d0cc379a94"></a>`owner`: `stove0_review_sampler_support.SamplerClient`
- <a id="s-1bdc547ec3"></a>`unit`: `member`

### Declared structure

- <a id="s-75ec928d9a"></a>`kind`: `"method"`
- <a id="s-522ac02830"></a>`signature`: `"\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""`

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_support.SamplerClient](stove0-review-sampler-support-samplerclient.md)

## Governing policies

- <a id="pa-6cff8a79af"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.SamplerClient.sample`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4664036c23ad91b550bae246387ac29d01adac41b2786346880ff16bf22501a4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "sample",
  "owner": "stove0_review_sampler_support.SamplerClient",
  "unit": "member"
}
```
