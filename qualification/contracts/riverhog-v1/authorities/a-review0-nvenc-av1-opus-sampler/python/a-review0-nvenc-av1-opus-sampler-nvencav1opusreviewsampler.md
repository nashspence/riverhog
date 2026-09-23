# a_review0_nvenc_av1_opus_sampler.NvencAv1OpusReviewSampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-nvenc-av1-opus-sampler:a-review0-nvenc-av1-opus-sampler-nvencav1-4ffe4fc219:73ae4c6ba8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-nvenc-av1-opus-sampler](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f643bc06cd"></a>
- <a id="s-5cc37be00b"></a>`distribution`: `a-review0-nvenc-av1-opus-sampler`
- <a id="s-e5a850835b"></a>`module`: `a_review0_nvenc_av1_opus_sampler`
- <a id="s-550556d569"></a>`name`: `NvencAv1OpusReviewSampler`
- <a id="s-d70a8a5f15"></a>`unit`: `export`

### Declared structure

- <a id="s-12bea33a53"></a>`kind`: `"class"`
- <a id="s-4a5542885a"></a>`signature`: `"\"(*, workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_id: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [descriptor](a-review0-nvenc-av1-opus-sampler-nvencav1opusreviewsampler-descriptor.md)
- [sample](a-review0-nvenc-av1-opus-sampler-nvencav1opusreviewsampler-sample.md)

## Governing policies

- <a id="pa-1081116ae7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-nvenc-av1-opus-sampler:a_review0_nvenc_av1_opus_sampler](../../../evidence/sources/authorities.md#src-b999ee5039) — [some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a\_review0\_nvenc\_av1\_opus\_sampler/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_nvenc_av1_opus_sampler.NvencAv1OpusReviewSampler`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a67f749b841f7b2bc5095ddcd6e35cd0f33e5716ecfbea032323b329bef748ba -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_id: 'str') -> 'None'\""
  },
  "distribution": "a-review0-nvenc-av1-opus-sampler",
  "module": "a_review0_nvenc_av1_opus_sampler",
  "name": "NvencAv1OpusReviewSampler",
  "unit": "export"
}
```

</details>
