# stove0_review_sampler_support.ReviewSampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-reviewsampler:08583fa1c9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eb4d37062a"></a>
| Field | Shape |
|---|---|
| <a id="s-0d4ef9ebdf"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-032266dbd4"></a>`distribution` | "stove0-review-sampler-support" |
| <a id="s-2ceb97a898"></a>`module` | "stove0_review_sampler_support" |
| <a id="s-439d338a18"></a>`name` | "ReviewSampler" |
| <a id="s-c6df19eeab"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_support.ReviewSampler.descriptor](stove0-review-sampler-support-reviewsampler-descriptor.md)
- [stove0_review_sampler_support.ReviewSampler.sample](stove0-review-sampler-support-reviewsampler-sample.md)

## Governing policies

- <a id="pa-53a1de87d9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.ReviewSampler`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f66ff78d6af059959cf3967b949ddfc096f668c27eccf2e34c5a72cd8a33707 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "ReviewSampler",
  "unit": "export"
}
```
