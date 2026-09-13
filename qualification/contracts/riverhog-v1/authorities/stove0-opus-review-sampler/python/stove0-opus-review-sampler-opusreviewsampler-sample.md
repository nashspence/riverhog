# stove0_opus_review_sampler.OpusReviewSampler.sample

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-review-sampler:stove0-opus-review-sampler-opusreviewsampler-sample:430b0a99c5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-50c0f45a12"></a>
| Field | Shape |
|---|---|
| <a id="s-4bdb4f4e91"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-999e4043a3"></a>`distribution` | "stove0-opus-review-sampler" |
| <a id="s-3f88677068"></a>`module` | "stove0_opus_review_sampler" |
| <a id="s-13072dbaf1"></a>`name` | "sample" |
| <a id="s-ba20ed340b"></a>`owner` | "stove0_opus_review_sampler.OpusReviewSampler" |
| <a id="s-20b779410d"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_opus_review_sampler.OpusReviewSampler](stove0-opus-review-sampler-opusreviewsampler.md)

## Governing policies

- <a id="pa-9977cfa8ee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-opus-review-sampler:stove0_opus_review_sampler](../../../evidence/sources.md#src-8c27cdb902) — `reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/__init__.py`

### Machine authority

- `/external_contract/python/stove0_opus_review_sampler.OpusReviewSampler.sample`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 721ea7c476076f1d0ad06c8f3d6291ff070897993b38b2a01d6573b29bf62661 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""
  },
  "distribution": "stove0-opus-review-sampler",
  "module": "stove0_opus_review_sampler",
  "name": "sample",
  "owner": "stove0_opus_review_sampler.OpusReviewSampler",
  "unit": "member"
}
```
