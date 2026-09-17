# stove0_review_sampler_protocol.SamplerOutput.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerout-785a0cc05c:062daf9832 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e5c26af118"></a>
- <a id="s-657db3f338"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-4976125c19"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-284c83d310"></a>`name`: `canonical_path`
- <a id="s-24d6619918"></a>`owner`: `stove0_review_sampler_protocol.SamplerOutput`
- <a id="s-12d0e1b506"></a>`unit`: `member`

### Declared structure

- <a id="s-c348205006"></a>`kind`: `"classmethod"`
- <a id="s-4a4810f10b"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [SamplerOutput](stove0-review-sampler-protocol-sampleroutput.md)

## Governing policies

- <a id="pa-2fb2a6280e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources/authorities.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerOutput.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0c8f42d69e337e1a5236fc09338ebe605349affcf33c018aec4094522ef46d2c -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "canonical_path",
  "owner": "stove0_review_sampler_protocol.SamplerOutput",
  "unit": "member"
}
```

</details>
