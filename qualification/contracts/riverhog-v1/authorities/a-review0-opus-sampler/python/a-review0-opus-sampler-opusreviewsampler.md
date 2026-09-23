# a_review0_opus_sampler.OpusReviewSampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-review0-opus-sampler:a-review0-opus-sampler-opusreviewsampler:b5dcf1d9d7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-opus-sampler](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-079aa7d188"></a>
- <a id="s-99ee2b4ef3"></a>`distribution`: `a-review0-opus-sampler`
- <a id="s-92efe6c6a9"></a>`module`: `a_review0_opus_sampler`
- <a id="s-2d3324ae97"></a>`name`: `OpusReviewSampler`
- <a id="s-f3e384c0f6"></a>`unit`: `export`

### Declared structure

- <a id="s-91d8d14b08"></a>`kind`: `"class"`
- <a id="s-b56b2eb184"></a>`signature`: `"\"(*, workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_digest: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [descriptor](a-review0-opus-sampler-opusreviewsampler-descriptor.md)
- [sample](a-review0-opus-sampler-opusreviewsampler-sample.md)

## Governing policies

- <a id="pa-2e9786379a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-review0-opus-sampler:a_review0_opus_sampler](../../../evidence/sources/authorities.md#src-7628caf8f5) — [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/__init__.py)

### Machine authority

- `/external_contract/python/a_review0_opus_sampler.OpusReviewSampler`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c38a4e67ab6590389aa9283e8339769b22150b8f03bf657437ca2d88f4648a7a -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_digest: 'str') -> 'None'\""
  },
  "distribution": "a-review0-opus-sampler",
  "module": "a_review0_opus_sampler",
  "name": "OpusReviewSampler",
  "unit": "export"
}
```

</details>
