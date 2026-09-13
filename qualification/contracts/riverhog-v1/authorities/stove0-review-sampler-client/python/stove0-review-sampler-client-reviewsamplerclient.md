# stove0_review_sampler_client.ReviewSamplerClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-client:stove0-review-sampler-client-reviewsamplerclient:bca0907416 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0dc4db8cc3"></a>
| Field | Shape |
|---|---|
| <a id="s-dd10fe606f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d37a678230"></a>`distribution` | "stove0-review-sampler-client" |
| <a id="s-e1c2a3421e"></a>`module` | "stove0_review_sampler_client" |
| <a id="s-33f9476ed5"></a>`name` | "ReviewSamplerClient" |
| <a id="s-daa2c5ec1e"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_client.ReviewSamplerClient.__enter__](stove0-review-sampler-client-reviewsamplerclient-enter.md)
- [stove0_review_sampler_client.ReviewSamplerClient.close](stove0-review-sampler-client-reviewsamplerclient-close.md)
- [stove0_review_sampler_client.ReviewSamplerClient.sample](stove0-review-sampler-client-reviewsamplerclient-sample.md)
- [stove0_review_sampler_client.ReviewSamplerClient.descriptor](stove0-review-sampler-client-reviewsamplerclient-descriptor.md)
- [stove0_review_sampler_client.ReviewSamplerClient.__exit__](stove0-review-sampler-client-reviewsamplerclient-exit.md)

## Governing policies

- <a id="pa-4879b912f1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-client:stove0_review_sampler_client](../../../evidence/sources.md#src-4a777c675f) — `reference/stove0/targets/review/sampler/client/src/stove0_review_sampler_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_client.ReviewSamplerClient`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 43a52696b931cd8306e28c8c92b62d76a66e278e8e80ba48a52568b237cc7c84 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(base_url: 'str', token: 'str', *, allow_insecure_http: 'bool' = False, timeout_seconds: 'float' = 86400, transport: 'httpx.BaseTransport | None' = None) -> 'None'\""
  },
  "distribution": "stove0-review-sampler-client",
  "module": "stove0_review_sampler_client",
  "name": "ReviewSamplerClient",
  "unit": "export"
}
```
