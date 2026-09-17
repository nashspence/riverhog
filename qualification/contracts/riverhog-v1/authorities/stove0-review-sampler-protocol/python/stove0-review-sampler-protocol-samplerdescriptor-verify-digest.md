# stove0_review_sampler_protocol.SamplerDescriptor.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerdes-c672819cb1:f8d9c9abcc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3b41c14610"></a>
- <a id="s-a24934aa9b"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-6b1a73f710"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-949ca5b5c5"></a>`name`: `verify_digest`
- <a id="s-3de020beb3"></a>`owner`: `stove0_review_sampler_protocol.SamplerDescriptor`
- <a id="s-21029544d0"></a>`unit`: `member`

### Declared structure

- <a id="s-24148905e9"></a>`kind`: `"method"`
- <a id="s-7e0edc7b3a"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [SamplerDescriptor](stove0-review-sampler-protocol-samplerdescriptor.md)

## Governing policies

- <a id="pa-2ae3866441"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources/authorities.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerDescriptor.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d6bfd50bf52a5b459717ae92455fec91931982e0990857234905d785710bf350 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "verify_digest",
  "owner": "stove0_review_sampler_protocol.SamplerDescriptor",
  "unit": "member"
}
```

</details>
