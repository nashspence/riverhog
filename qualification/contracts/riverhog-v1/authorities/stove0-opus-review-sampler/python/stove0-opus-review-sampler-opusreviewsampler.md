# stove0_opus_review_sampler.OpusReviewSampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-review-sampler:stove0-opus-review-sampler-opusreviewsampler:77136577ca -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-de7aeac5f2"></a>
| Field | Shape |
|---|---|
| <a id="s-3d6bdf3580"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-619b7a6add"></a>`distribution` | "stove0-opus-review-sampler" |
| <a id="s-f3297768c8"></a>`module` | "stove0_opus_review_sampler" |
| <a id="s-5c1390c1f8"></a>`name` | "OpusReviewSampler" |
| <a id="s-ee1fa1459a"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_opus_review_sampler.OpusReviewSampler.descriptor](stove0-opus-review-sampler-opusreviewsampler-descriptor.md)
- [stove0_opus_review_sampler.OpusReviewSampler.sample](stove0-opus-review-sampler-opusreviewsampler-sample.md)

## Governing policies

- <a id="pa-512a03ea49"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-opus-review-sampler:stove0_opus_review_sampler](../../../evidence/sources.md#src-8c27cdb902) — `reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/__init__.py`

### Machine authority

- `/external_contract/python/stove0_opus_review_sampler.OpusReviewSampler`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 47ea1370c04ac2652e1764fab2fa25bc4aa3962f2b85d8f41e0c507969e0a442 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_digest: 'str') -> 'None'\""
  },
  "distribution": "stove0-opus-review-sampler",
  "module": "stove0_opus_review_sampler",
  "name": "OpusReviewSampler",
  "unit": "export"
}
```
