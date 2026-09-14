# stove0_review_sampler_protocol.SamplerResult.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerresult-seal:006d596452 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-44d37923b2"></a>
- <a id="s-6a61538436"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-13865a3606"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-be624d551f"></a>`name`: `seal`
- <a id="s-c0249317c7"></a>`owner`: `stove0_review_sampler_protocol.SamplerResult`
- <a id="s-465c577a92"></a>`unit`: `member`

### Declared structure

- <a id="s-6e19191d48"></a>`kind`: `"classmethod"`
- <a id="s-e2e12a70cd"></a>`signature`: `"\"(cls, payload: 'SamplerResultPayload') -> 'SamplerResult'\""`

## Maintained corroboration

### Related interface records

- [SamplerResult](stove0-review-sampler-protocol-samplerresult.md)

## Governing policies

- <a id="pa-fa6ccd69fb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerResult.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b14d5fc163b927f7fc641bf94cd288e74158e48a3f1300cd9d9a35c9639d780e -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'SamplerResultPayload') -> 'SamplerResult'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "seal",
  "owner": "stove0_review_sampler_protocol.SamplerResult",
  "unit": "member"
}
```
