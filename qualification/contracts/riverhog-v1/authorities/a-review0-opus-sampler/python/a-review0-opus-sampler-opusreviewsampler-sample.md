# a_review0_opus_sampler.OpusReviewSampler.sample

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-opus-sampler:a-review0-opus-sampler-opusreviewsampler-sample:1b0d090fca -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-opus-sampler](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f0e0f1e1d6"></a>
- <a id="s-e05f6312bc"></a>`distribution`: `a-review0-opus-sampler`
- <a id="s-8ecd7f98c4"></a>`module`: `a_review0_opus_sampler`
- <a id="s-f23e432316"></a>`name`: `sample`
- <a id="s-2569976ab4"></a>`owner`: `a_review0_opus_sampler.OpusReviewSampler`
- <a id="s-2bb13179ca"></a>`unit`: `member`

### Declared structure

- <a id="s-b1ce9b7b79"></a>`kind`: `"method"`
- <a id="s-87a3a27cdf"></a>`signature`: `"\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""`

## Maintained corroboration

### Related interface records

- [OpusReviewSampler](a-review0-opus-sampler-opusreviewsampler.md)

## Governing policies

- <a id="pa-dd970bc5cd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-opus-sampler:a_review0_opus_sampler](../../../evidence/sources/authorities.md#src-7628caf8f5) — [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_opus_sampler.OpusReviewSampler.sample`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c5e9dbbfe5adec8682b316954b57282621ffb3bf80ad425be00b29b1cd9c79d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""
  },
  "distribution": "a-review0-opus-sampler",
  "module": "a_review0_opus_sampler",
  "name": "sample",
  "owner": "a_review0_opus_sampler.OpusReviewSampler",
  "unit": "member"
}
```

</details>
