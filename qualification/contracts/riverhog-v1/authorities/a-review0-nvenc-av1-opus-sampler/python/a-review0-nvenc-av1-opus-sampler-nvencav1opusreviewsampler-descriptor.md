# a_review0_nvenc_av1_opus_sampler.NvencAv1OpusReviewSampler.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-nvenc-av1-opus-sampler:a-review0-nvenc-av1-opus-sampler-nvencav1-67a3bc29d8:21193f99f8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-nvenc-av1-opus-sampler](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-521eb122f8"></a>
- <a id="s-0f7f81c9e3"></a>`distribution`: `a-review0-nvenc-av1-opus-sampler`
- <a id="s-f7627c4cd5"></a>`module`: `a_review0_nvenc_av1_opus_sampler`
- <a id="s-068dad513e"></a>`name`: `descriptor`
- <a id="s-ef50cbbd6e"></a>`owner`: `a_review0_nvenc_av1_opus_sampler.NvencAv1OpusReviewSampler`
- <a id="s-87606fb468"></a>`unit`: `member`

### Declared structure

- <a id="s-f995061054"></a>`kind`: `"method"`
- <a id="s-50394bc7dd"></a>`signature`: `"\"(self) -> 'SamplerDescriptor'\""`

## Maintained corroboration

### Related interface records

- [NvencAv1OpusReviewSampler](a-review0-nvenc-av1-opus-sampler-nvencav1opusreviewsampler.md)

## Governing policies

- <a id="pa-aa1f5ee76e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-nvenc-av1-opus-sampler:a_review0_nvenc_av1_opus_sampler](../../../evidence/sources/authorities.md#src-b999ee5039) — [some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a\_review0\_nvenc\_av1\_opus\_sampler/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_nvenc_av1_opus_sampler.NvencAv1OpusReviewSampler.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d7a27d7d668880ed5bfc464849708b44f611bff1dc01b6a6ed101bdb213aa1db -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'SamplerDescriptor'\""
  },
  "distribution": "a-review0-nvenc-av1-opus-sampler",
  "module": "a_review0_nvenc_av1_opus_sampler",
  "name": "descriptor",
  "owner": "a_review0_nvenc_av1_opus_sampler.NvencAv1OpusReviewSampler",
  "unit": "member"
}
```

</details>
