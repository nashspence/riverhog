# stove0_review_sampler_client.ReviewSamplerClient.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-client:stove0-review-sampler-client-reviewsample-c7c991c800:f6ce4b2b18 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-de66e2513e"></a>
- <a id="s-0d83704a14"></a>`distribution`: `stove0-review-sampler-client`
- <a id="s-c07118def7"></a>`module`: `stove0_review_sampler_client`
- <a id="s-0bf71ff145"></a>`name`: `descriptor`
- <a id="s-62e6b426eb"></a>`owner`: `stove0_review_sampler_client.ReviewSamplerClient`
- <a id="s-7829c706ac"></a>`unit`: `member`

### Declared structure

- <a id="s-8f531c3b72"></a>`kind`: `"method"`
- <a id="s-50e59fe5f3"></a>`signature`: `"\"(self, *, refresh: 'bool' = False) -> 'SamplerDescriptor'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplerClient](stove0-review-sampler-client-reviewsamplerclient.md)

## Governing policies

- <a id="pa-b24de16af9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-client:stove0_review_sampler_client](../../../evidence/sources.md#src-4a777c675f) — `reference/stove0/targets/review/sampler/client/src/stove0_review_sampler_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_client.ReviewSamplerClient.descriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f86017b2105ff6042af415589bd99aa8db060dda09bf63ce201efb85b156121 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, refresh: 'bool' = False) -> 'SamplerDescriptor'\""
  },
  "distribution": "stove0-review-sampler-client",
  "module": "stove0_review_sampler_client",
  "name": "descriptor",
  "owner": "stove0_review_sampler_client.ReviewSamplerClient",
  "unit": "member"
}
```
