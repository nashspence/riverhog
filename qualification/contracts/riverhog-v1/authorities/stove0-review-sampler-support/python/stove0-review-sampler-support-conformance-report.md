# stove0_review_sampler_support.conformance_report

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-conformance-report:22bcf7c6ae -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fa713674bc"></a>
- <a id="s-9c2cb1798e"></a>`distribution`: `stove0-review-sampler-support`
- <a id="s-44ea749f32"></a>`module`: `stove0_review_sampler_support`
- <a id="s-4a14361e31"></a>`name`: `conformance_report`
- <a id="s-2e53f69268"></a>`unit`: `export`

### Declared structure

- <a id="s-931135eb46"></a>`kind`: `"function"`
- <a id="s-8a2a7ad3a8"></a>`signature`: `"\"(client: 'SamplerClient', *, request: 'SamplerRequest \| None' = None) -> 'SamplerConformanceResult'\""`

## Governing policies

- <a id="pa-3871e63ba9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources/authorities.md#src-6dd798b0df) — [reference/stove0/targets/review/sampler/support/src/stove0\_review\_sampler\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.conformance_report`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7db318b619bc6ba111bd5ea917da73b16c8a56ad43ae0f1e2832d9fa4874ace7 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(client: 'SamplerClient', *, request: 'SamplerRequest | None' = None) -> 'SamplerConformanceResult'\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "conformance_report",
  "unit": "export"
}
```

</details>
