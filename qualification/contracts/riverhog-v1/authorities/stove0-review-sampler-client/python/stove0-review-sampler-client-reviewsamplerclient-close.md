# stove0_review_sampler_client.ReviewSamplerClient.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-client:stove0-review-sampler-client-reviewsample-a0c4e67aa2:7d57207679 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7108135aa7"></a>
- <a id="s-464d9d1699"></a>`distribution`: `stove0-review-sampler-client`
- <a id="s-1f0b6cd648"></a>`module`: `stove0_review_sampler_client`
- <a id="s-1dd1c88691"></a>`name`: `close`
- <a id="s-e30681a579"></a>`owner`: `stove0_review_sampler_client.ReviewSamplerClient`
- <a id="s-d3202cbcda"></a>`unit`: `member`

### Declared structure

- <a id="s-64b4d2d46c"></a>`kind`: `"method"`
- <a id="s-e4a0da13c2"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplerClient](stove0-review-sampler-client-reviewsamplerclient.md)

## Governing policies

- <a id="pa-6957934592"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-client:stove0_review_sampler_client](../../../evidence/sources/authorities.md#src-4a777c675f) — [reference/stove0/targets/review/sampler/client/src/stove0\_review\_sampler\_client/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/client/src/stove0_review_sampler_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_client.ReviewSamplerClient.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
