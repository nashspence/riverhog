# stove0_opus_review_sampler.OpusReviewSampler.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-review-sampler:stove0-opus-review-sampler-opusreviewsamp-892ed3336f:2ec7813070 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-06f732b19e"></a>
| Field | Shape |
|---|---|
| <a id="s-403ba6460f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-9178399e90"></a>`distribution` | "stove0-opus-review-sampler" |
| <a id="s-196bac390d"></a>`module` | "stove0_opus_review_sampler" |
| <a id="s-c358fb48ac"></a>`name` | "descriptor" |
| <a id="s-c58db32e7e"></a>`owner` | "stove0_opus_review_sampler.OpusReviewSampler" |
| <a id="s-a4c106dba7"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_opus_review_sampler.OpusReviewSampler](stove0-opus-review-sampler-opusreviewsampler.md)

## Governing policies

- <a id="pa-3ea77508ce"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-opus-review-sampler:stove0_opus_review_sampler](../../../evidence/sources.md#src-8c27cdb902) — `reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/__init__.py`

### Machine authority

- `/external_contract/python/stove0_opus_review_sampler.OpusReviewSampler.descriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 16ffc363dd007063c97cbe4df02b5a7ba05748d25a864060b53eca8314cb2cab -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'SamplerDescriptor'\""
  },
  "distribution": "stove0-opus-review-sampler",
  "module": "stove0_opus_review_sampler",
  "name": "descriptor",
  "owner": "stove0_opus_review_sampler.OpusReviewSampler",
  "unit": "member"
}
```
