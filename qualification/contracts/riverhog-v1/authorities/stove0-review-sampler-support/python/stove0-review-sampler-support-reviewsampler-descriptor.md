# stove0_review_sampler_support.ReviewSampler.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-reviewsampl-62d697f26b:550744e273 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f3f0d7221"></a>
| Field | Shape |
|---|---|
| <a id="s-ef4be284c4"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-7dac8f9704"></a>`distribution` | "stove0-review-sampler-support" |
| <a id="s-4f8cf70a9a"></a>`module` | "stove0_review_sampler_support" |
| <a id="s-da19c71874"></a>`name` | "descriptor" |
| <a id="s-a82ddc9321"></a>`owner` | "stove0_review_sampler_support.ReviewSampler" |
| <a id="s-92ff2698c5"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_support.ReviewSampler](stove0-review-sampler-support-reviewsampler.md)

## Governing policies

- <a id="pa-f3cf52043a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.ReviewSampler.descriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c9fed013f446ebf9aeb9781aa6c332a74a351ff8819bf8c3697a9ec623b4fbb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'SamplerDescriptor'\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "descriptor",
  "owner": "stove0_review_sampler_support.ReviewSampler",
  "unit": "member"
}
```
