# stove0_review_sampler_support.SamplerHttpBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-samplerhttpbinding:59c7de845a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-07b66c4a3f"></a>
- <a id="s-3ef2005b72"></a>`distribution`: `stove0-review-sampler-support`
- <a id="s-8fa37eeb3f"></a>`module`: `stove0_review_sampler_support`
- <a id="s-140fac9c39"></a>`name`: `SamplerHttpBinding`
- <a id="s-7e28281d9f"></a>`unit`: `export`

### Declared structure

- <a id="s-8cbbba8169"></a>`kind`: `"class"`
- <a id="s-39702f84c3"></a>`signature`: `"\"(sampler: 'ReviewSampler', *, maximum_request_bytes: 'int' = 4194304, maximum_concurrency: 'int' = 1) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [handle](stove0-review-sampler-support-samplerhttpbinding-handle.md)

## Governing policies

- <a id="pa-7f38c9569e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources/authorities.md#src-6dd798b0df) — [reference/stove0/targets/review/sampler/support/src/stove0\_review\_sampler\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.SamplerHttpBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 54276fa3bf43e0dbe08f3a2c484b9887e52b5331befc62c9562587b13eefa3ed -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(sampler: 'ReviewSampler', *, maximum_request_bytes: 'int' = 4194304, maximum_concurrency: 'int' = 1) -> 'None'\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "SamplerHttpBinding",
  "unit": "export"
}
```

</details>
