# stove0_nvenc_av1_opus_review_sampler.NvencAv1OpusReviewSampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler-nven-6e81028c70:b21ff10940 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2e95753161"></a>
- <a id="s-3e39d9b473"></a>`distribution`: `stove0-nvenc-av1-opus-review-sampler`
- <a id="s-c07781ca4d"></a>`module`: `stove0_nvenc_av1_opus_review_sampler`
- <a id="s-c861155dcb"></a>`name`: `NvencAv1OpusReviewSampler`
- <a id="s-bd43e666a7"></a>`unit`: `export`

### Declared structure

- <a id="s-855b635492"></a>`kind`: `"class"`
- <a id="s-5776c4ce4c"></a>`signature`: `"\"(*, workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_digest: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [descriptor](stove0-nvenc-av1-opus-review-sampler-nvencav1opusreviewsampler-descriptor.md)
- [sample](stove0-nvenc-av1-opus-review-sampler-nvencav1opusreviewsampler-sample.md)

## Governing policies

- <a id="pa-0216486e57"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-nvenc-av1-opus-review-sampler:stove0_nvenc_av1_opus_review_sampler](../../../evidence/sources.md#src-dc2d09f8b3) — `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/__init__.py`

### Machine authority

- `/external_contract/python/stove0_nvenc_av1_opus_review_sampler.NvencAv1OpusReviewSampler`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ec81709b977e9765bda18d91d645106d18a139268b5535d2737c065ebcf596c6 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_digest: 'str') -> 'None'\""
  },
  "distribution": "stove0-nvenc-av1-opus-review-sampler",
  "module": "stove0_nvenc_av1_opus_review_sampler",
  "name": "NvencAv1OpusReviewSampler",
  "unit": "export"
}
```

</details>
