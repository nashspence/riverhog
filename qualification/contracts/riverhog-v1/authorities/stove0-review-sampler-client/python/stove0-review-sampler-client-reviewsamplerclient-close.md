# stove0_review_sampler_client.ReviewSamplerClient.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-client:stove0-review-sampler-client-reviewsample-a0c4e67aa2:7d57207679 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7108135aa7"></a>
| Field | Shape |
|---|---|
| <a id="s-e41a759e66"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-464d9d1699"></a>`distribution` | "stove0-review-sampler-client" |
| <a id="s-1f0b6cd648"></a>`module` | "stove0_review_sampler_client" |
| <a id="s-1dd1c88691"></a>`name` | "close" |
| <a id="s-e30681a579"></a>`owner` | "stove0_review_sampler_client.ReviewSamplerClient" |
| <a id="s-d3202cbcda"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_client.ReviewSamplerClient](stove0-review-sampler-client-reviewsamplerclient.md)

## Governing policies

- <a id="pa-6957934592"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-client:stove0_review_sampler_client](../../../evidence/sources.md#src-4a777c675f) — `reference/stove0/targets/review/sampler/client/src/stove0_review_sampler_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_client.ReviewSamplerClient.close`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4934f17f1f1c9c34829e4f403fecf1ad144ea811f644d7d813aff5d0779c4cc1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "stove0-review-sampler-client",
  "module": "stove0_review_sampler_client",
  "name": "close",
  "owner": "stove0_review_sampler_client.ReviewSamplerClient",
  "unit": "member"
}
```
