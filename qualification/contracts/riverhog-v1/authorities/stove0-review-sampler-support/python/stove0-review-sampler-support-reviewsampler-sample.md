# stove0_review_sampler_support.ReviewSampler.sample

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-reviewsampler-sample:88bc4680c5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f59a6b1129"></a>
- <a id="s-e9311ac2cd"></a>`distribution`: `stove0-review-sampler-support`
- <a id="s-9e2b03b462"></a>`module`: `stove0_review_sampler_support`
- <a id="s-96c797f3cb"></a>`name`: `sample`
- <a id="s-3ab32db323"></a>`owner`: `stove0_review_sampler_support.ReviewSampler`
- <a id="s-c08ad55170"></a>`unit`: `member`

### Declared structure

- <a id="s-ce275c0323"></a>`kind`: `"method"`
- <a id="s-491f1245cd"></a>`signature`: `"\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""`

## Maintained corroboration

### Related interface records

- [ReviewSampler](stove0-review-sampler-support-reviewsampler.md)

## Governing policies

- <a id="pa-61a095d3bd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources/authorities.md#src-6dd798b0df) — [reference/stove0/targets/review/sampler/support/src/stove0\_review\_sampler\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.ReviewSampler.sample`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa1800d785a267f9fd4da302e9126a892e22794058a83419fc585e6f052ef5a0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "sample",
  "owner": "stove0_review_sampler_support.ReviewSampler",
  "unit": "member"
}
```

</details>
