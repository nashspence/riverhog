# stove0_review_sampler_protocol.SamplerRequest.canonical_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerreq-61ac76128b:1cdf1479c6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a14bdf81f4"></a>
- <a id="s-2a35ea15f5"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-01a0d6cfdd"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-d1e8be617c"></a>`name`: `canonical_inputs`
- <a id="s-003e03e3f5"></a>`owner`: `stove0_review_sampler_protocol.SamplerRequest`
- <a id="s-f333ff199a"></a>`unit`: `member`

### Declared structure

- <a id="s-9a5a122c99"></a>`kind`: `"classmethod"`
- <a id="s-762e000b3a"></a>`signature`: `"\"(cls, value: 'tuple[SamplerInput, ...]') -> 'tuple[SamplerInput, ...]'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequest](stove0-review-sampler-protocol-samplerrequest.md)

## Governing policies

- <a id="pa-fc36e32eb5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerRequest.canonical_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7481e7eab59afef321860cae1c60bc73984b8260d80ee09d85723832925ffda4 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SamplerInput, ...]') -> 'tuple[SamplerInput, ...]'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "canonical_inputs",
  "owner": "stove0_review_sampler_protocol.SamplerRequest",
  "unit": "member"
}
```

</details>
