# a_review0_opus_sampler.OpusReviewSampler.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-opus-sampler:a-review0-opus-sampler-opusreviewsampler-descriptor:284e8bda7c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-opus-sampler](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c426a128da"></a>
- <a id="s-32a88bdfaa"></a>`distribution`: `a-review0-opus-sampler`
- <a id="s-bfe395150f"></a>`module`: `a_review0_opus_sampler`
- <a id="s-7c7e0334de"></a>`name`: `descriptor`
- <a id="s-0ae101cee2"></a>`owner`: `a_review0_opus_sampler.OpusReviewSampler`
- <a id="s-f18a0de4ea"></a>`unit`: `member`

### Declared structure

- <a id="s-94b386c122"></a>`kind`: `"method"`
- <a id="s-85eb28d17a"></a>`signature`: `"\"(self) -> 'SamplerDescriptor'\""`

## Maintained corroboration

### Related interface records

- [OpusReviewSampler](a-review0-opus-sampler-opusreviewsampler.md)

## Governing policies

- <a id="pa-057cb58692"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-opus-sampler:a_review0_opus_sampler](../../../evidence/sources/authorities.md#src-7628caf8f5) — [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_opus_sampler.OpusReviewSampler.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c465d57fff400a4ab31c6f6f31a0f81a5e464f87fef063b536a8a8675754717 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'SamplerDescriptor'\""
  },
  "distribution": "a-review0-opus-sampler",
  "module": "a_review0_opus_sampler",
  "name": "descriptor",
  "owner": "a_review0_opus_sampler.OpusReviewSampler",
  "unit": "member"
}
```

</details>
