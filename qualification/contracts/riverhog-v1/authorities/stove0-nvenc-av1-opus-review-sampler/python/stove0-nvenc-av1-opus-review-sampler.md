# stove0_nvenc_av1_opus_review_sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler:78913821ec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8e24c9d358"></a>
| Field | Shape |
|---|---|
| <a id="s-4131dc417c"></a>`candidate_id` | "python:stove0-nvenc-av1-opus-review-sampler:stove0_nvenc_av1_opus_review_sampler" |
| <a id="s-5dac6bc446"></a>`distribution` | "stove0-nvenc-av1-opus-review-sampler" |
| <a id="s-0f8fba37b4"></a>`exports` | additional keys=`NvencAv1OpusReviewSampler` |
| <a id="s-fc50c92cef"></a>`module` | "stove0_nvenc_av1_opus_review_sampler" |

## Governing policies

- <a id="pa-c62e9e94cf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-nvenc-av1-opus-review-sampler:stove0_nvenc_av1_opus_review_sampler](../../../evidence/sources.md#src-dc2d09f8b3) — `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/__init__.py`

### Machine authority

- `/external_contract/python/39`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0897f51e921c8d4033c095a7b6078c1ddf9a690d7d0af26d77f46964fad7179b -->

```json
{
  "candidate_id": "python:stove0-nvenc-av1-opus-review-sampler:stove0_nvenc_av1_opus_review_sampler",
  "distribution": "stove0-nvenc-av1-opus-review-sampler",
  "exports": {
    "NvencAv1OpusReviewSampler": {
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
  "module": "stove0_nvenc_av1_opus_review_sampler"
}
```
