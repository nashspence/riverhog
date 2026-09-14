# stove0_review_sampler_client.ReviewSamplerClient.sample

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-client:stove0-review-sampler-client-reviewsample-ae09c4f2a8:4cb5adb056 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0c6051b6b5"></a>
- <a id="s-6d785e9e59"></a>`distribution`: `stove0-review-sampler-client`
- <a id="s-e23c75dca2"></a>`module`: `stove0_review_sampler_client`
- <a id="s-50ad8084db"></a>`name`: `sample`
- <a id="s-36a9e2e191"></a>`owner`: `stove0_review_sampler_client.ReviewSamplerClient`
- <a id="s-035bc47835"></a>`unit`: `member`

### Declared structure

- <a id="s-70e70c9801"></a>`kind`: `"method"`
- <a id="s-b2551a294d"></a>`signature`: `"\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplerClient](stove0-review-sampler-client-reviewsamplerclient.md)

## Governing policies

- <a id="pa-3e6e2fd806"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-client:stove0_review_sampler_client](../../../evidence/sources.md#src-4a777c675f) — `reference/stove0/targets/review/sampler/client/src/stove0_review_sampler_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_client.ReviewSamplerClient.sample`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eda4e8382ba62415e3ac39ce80be2be45e7d6c41164540c5945140fd8f6ea928 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""
  },
  "distribution": "stove0-review-sampler-client",
  "module": "stove0_review_sampler_client",
  "name": "sample",
  "owner": "stove0_review_sampler_client.ReviewSamplerClient",
  "unit": "member"
}
```
