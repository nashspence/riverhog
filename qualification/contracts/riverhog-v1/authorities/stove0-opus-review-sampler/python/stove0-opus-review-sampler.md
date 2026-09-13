# stove0_opus_review_sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-review-sampler:stove0-opus-review-sampler:1973d77f29 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-7ec8f71718) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e4aa572918"></a>
| Field | Shape |
|---|---|
| <a id="s-ffb6588577"></a>`candidate_id` | "python:stove0-opus-review-sampler:stove0_opus_review_sampler" |
| <a id="s-f6365f5ab9"></a>`distribution` | "stove0-opus-review-sampler" |
| <a id="s-7d79d584f6"></a>`exports` | additional keys=`OpusReviewSampler` |
| <a id="s-f1836c8e0d"></a>`module` | "stove0_opus_review_sampler" |

## Governing policies

- <a id="pa-6f8a5e9080"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-opus-review-sampler:stove0_opus_review_sampler](../../../evidence/sources.md#src-8c27cdb902) — `reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/__init__.py`

### Machine authority

- `/external_contract/python/45`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 32798e2673cadc916e8e8152354735728e95eae6c1078635f8077302b855bd15 -->

```json
{
  "candidate_id": "python:stove0-opus-review-sampler:stove0_opus_review_sampler",
  "distribution": "stove0-opus-review-sampler",
  "exports": {
    "OpusReviewSampler": {
      "kind": "class",
      "members": {
        "descriptor": {
          "kind": "method",
          "signature": "\"(self) -> 'SamplerDescriptor'\""
        },
        "sample": {
          "kind": "method",
          "signature": "\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""
        }
      },
      "signature": "\"(*, workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_digest: 'str') -> 'None'\""
    }
  },
  "module": "stove0_opus_review_sampler"
}
```
