# stove0_review_sampler_protocol.SamplerRequestPayload.canonical_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerreq-f33fa4d5b1:cfc5d4a4e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b1d0a71bba"></a>
- <a id="s-6cc295b82d"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-8fa4bbdc45"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-e533c3b623"></a>`name`: `canonical_inputs`
- <a id="s-2891d4fe70"></a>`owner`: `stove0_review_sampler_protocol.SamplerRequestPayload`
- <a id="s-e583889c2d"></a>`unit`: `member`

### Declared structure

- <a id="s-47b232e16e"></a>`kind`: `"classmethod"`
- <a id="s-ff97c1b649"></a>`signature`: `"\"(cls, value: 'tuple[SamplerInput, ...]') -> 'tuple[SamplerInput, ...]'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequestPayload](stove0-review-sampler-protocol-samplerrequestpayload.md)

## Governing policies

- <a id="pa-4df617b7c0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerRequestPayload.canonical_inputs`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d406d676baae6a4553ee7031896e34f530c8a9ccc469abfb125c0b9864dff944 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SamplerInput, ...]') -> 'tuple[SamplerInput, ...]'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "canonical_inputs",
  "owner": "stove0_review_sampler_protocol.SamplerRequestPayload",
  "unit": "member"
}
```
