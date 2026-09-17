# stove0_review_sampler_protocol.SamplerRequestPayload.canonical_windows

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerreq-f9c483f12e:89dc3f6b8d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4bd1eb8252"></a>
- <a id="s-593a3a7d31"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-5710779271"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-b94881234e"></a>`name`: `canonical_windows`
- <a id="s-83c908e812"></a>`owner`: `stove0_review_sampler_protocol.SamplerRequestPayload`
- <a id="s-3c781563f2"></a>`unit`: `member`

### Declared structure

- <a id="s-3038eda3b2"></a>`kind`: `"classmethod"`
- <a id="s-071f915c56"></a>`signature`: `"\"(cls, value: 'tuple[SamplerWindow, ...]') -> 'tuple[SamplerWindow, ...]'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequestPayload](stove0-review-sampler-protocol-samplerrequestpayload.md)

## Governing policies

- <a id="pa-7605de523f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerRequestPayload.canonical_windows`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40145ce6a06d18dac23a2153d2520098448fff84aa0f4194dd2737be554bc40b -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SamplerWindow, ...]') -> 'tuple[SamplerWindow, ...]'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "canonical_windows",
  "owner": "stove0_review_sampler_protocol.SamplerRequestPayload",
  "unit": "member"
}
```

</details>
