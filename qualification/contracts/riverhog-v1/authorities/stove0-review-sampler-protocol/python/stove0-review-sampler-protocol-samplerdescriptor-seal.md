# stove0_review_sampler_protocol.SamplerDescriptor.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerdes-734aaff04b:eaca3f8407 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ae52b84df4"></a>
- <a id="s-27ecbe966f"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-ee383440ae"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-e4278e41b2"></a>`name`: `seal`
- <a id="s-90de23bd83"></a>`owner`: `stove0_review_sampler_protocol.SamplerDescriptor`
- <a id="s-c54d5a3c6d"></a>`unit`: `member`

### Declared structure

- <a id="s-70a0304c70"></a>`kind`: `"classmethod"`
- <a id="s-2593d3f5fc"></a>`signature`: `"\"(cls, payload: 'SamplerDescriptorPayload') -> 'SamplerDescriptor'\""`

## Maintained corroboration

### Related interface records

- [SamplerDescriptor](stove0-review-sampler-protocol-samplerdescriptor.md)

## Governing policies

- <a id="pa-2b14932193"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerDescriptor.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d09c92e8b314b7a9b0be73ee27b47e3c00c8c59d7b9108d6bd44d89c39c53f94 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'SamplerDescriptorPayload') -> 'SamplerDescriptor'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "seal",
  "owner": "stove0_review_sampler_protocol.SamplerDescriptor",
  "unit": "member"
}
```

</details>
