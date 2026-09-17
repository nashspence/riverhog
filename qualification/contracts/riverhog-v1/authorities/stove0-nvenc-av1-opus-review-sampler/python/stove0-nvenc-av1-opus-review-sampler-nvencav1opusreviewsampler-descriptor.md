# stove0_nvenc_av1_opus_review_sampler.NvencAv1OpusReviewSampler.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler-nven-76d681f569:055951f10d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4f3ac3486"></a>
- <a id="s-d533161805"></a>`distribution`: `stove0-nvenc-av1-opus-review-sampler`
- <a id="s-67fdb82d4d"></a>`module`: `stove0_nvenc_av1_opus_review_sampler`
- <a id="s-12219f7a93"></a>`name`: `descriptor`
- <a id="s-44e5046432"></a>`owner`: `stove0_nvenc_av1_opus_review_sampler.NvencAv1OpusReviewSampler`
- <a id="s-a15b5c0f3f"></a>`unit`: `member`

### Declared structure

- <a id="s-7531145dbf"></a>`kind`: `"method"`
- <a id="s-8bc7d4ade7"></a>`signature`: `"\"(self) -> 'SamplerDescriptor'\""`

## Maintained corroboration

### Related interface records

- [NvencAv1OpusReviewSampler](stove0-nvenc-av1-opus-review-sampler-nvencav1opusreviewsampler.md)

## Governing policies

- <a id="pa-35c028c01e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-nvenc-av1-opus-review-sampler:stove0_nvenc_av1_opus_review_sampler](../../../evidence/sources/authorities.md#src-dc2d09f8b3) — [reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0\_nvenc\_av1\_opus\_review\_sampler/\_\_init\_\_.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/__init__.py)

### Machine authority

- `/external_contract/python/stove0_nvenc_av1_opus_review_sampler.NvencAv1OpusReviewSampler.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 684d82229718e4b5eb0cb723907268b1b78e5a05c1091a3e0971a0f82dac8e11 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'SamplerDescriptor'\""
  },
  "distribution": "stove0-nvenc-av1-opus-review-sampler",
  "module": "stove0_nvenc_av1_opus_review_sampler",
  "name": "descriptor",
  "owner": "stove0_nvenc_av1_opus_review_sampler.NvencAv1OpusReviewSampler",
  "unit": "member"
}
```

</details>
