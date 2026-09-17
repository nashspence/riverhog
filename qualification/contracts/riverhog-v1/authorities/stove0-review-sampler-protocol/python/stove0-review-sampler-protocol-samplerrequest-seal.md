# stove0_review_sampler_protocol.SamplerRequest.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerrequest-seal:b23d722e89 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-198993ef10"></a>
- <a id="s-f4c8f053c9"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-c9482083c0"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-4c30a46d2b"></a>`name`: `seal`
- <a id="s-efec6a9bc3"></a>`owner`: `stove0_review_sampler_protocol.SamplerRequest`
- <a id="s-8f648c6cfb"></a>`unit`: `member`

### Declared structure

- <a id="s-829ca19aa9"></a>`kind`: `"classmethod"`
- <a id="s-3498926c41"></a>`signature`: `"\"(cls, payload: 'SamplerRequestPayload') -> 'SamplerRequest'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequest](stove0-review-sampler-protocol-samplerrequest.md)

## Governing policies

- <a id="pa-1b22e4373f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerRequest.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 892866f59000a56e16ed8d1a7a8cce246a69a1a1c336fd95414c6978bbff4b0a -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'SamplerRequestPayload') -> 'SamplerRequest'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "seal",
  "owner": "stove0_review_sampler_protocol.SamplerRequest",
  "unit": "member"
}
```

</details>
