# stove0_nvenc_av1_opus_review_sampler.NvencAv1OpusReviewSampler.sample

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler-nven-ca22a238fd:e350ab002f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-250dbf92ad"></a>
- <a id="s-7d8c4e3c3f"></a>`distribution`: `stove0-nvenc-av1-opus-review-sampler`
- <a id="s-0df1d045db"></a>`module`: `stove0_nvenc_av1_opus_review_sampler`
- <a id="s-b65e183902"></a>`name`: `sample`
- <a id="s-e5247d0167"></a>`owner`: `stove0_nvenc_av1_opus_review_sampler.NvencAv1OpusReviewSampler`
- <a id="s-57735f9920"></a>`unit`: `member`

### Declared structure

- <a id="s-0996fc6679"></a>`kind`: `"method"`
- <a id="s-4a6db70cc0"></a>`signature`: `"\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""`

## Maintained corroboration

### Related interface records

- [NvencAv1OpusReviewSampler](stove0-nvenc-av1-opus-review-sampler-nvencav1opusreviewsampler.md)

## Governing policies

- <a id="pa-096a5d16ff"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-nvenc-av1-opus-review-sampler:stove0_nvenc_av1_opus_review_sampler](../../../evidence/sources/authorities.md#src-dc2d09f8b3) — [reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0\_nvenc\_av1\_opus\_review\_sampler/\_\_init\_\_.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/__init__.py)

### Machine authority

- `/external_contract/python/stove0_nvenc_av1_opus_review_sampler.NvencAv1OpusReviewSampler.sample`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 647508d431bdafb47c574f823c376e1c40765362f4c3aae8d1c4425f24577807 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""
  },
  "distribution": "stove0-nvenc-av1-opus-review-sampler",
  "module": "stove0_nvenc_av1_opus_review_sampler",
  "name": "sample",
  "owner": "stove0_nvenc_av1_opus_review_sampler.NvencAv1OpusReviewSampler",
  "unit": "member"
}
```

</details>
