# stove0_review_sampler_protocol.SamplerResultPayload.canonical_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerres-9de64de1f1:404c95c92f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-392927d76c"></a>
- <a id="s-ad3b9dc6e7"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-289b435519"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-359ed543e4"></a>`name`: `canonical_outputs`
- <a id="s-e0232c67cc"></a>`owner`: `stove0_review_sampler_protocol.SamplerResultPayload`
- <a id="s-2ef7881378"></a>`unit`: `member`

### Declared structure

- <a id="s-a92c38fbfc"></a>`kind`: `"classmethod"`
- <a id="s-a4fe085dc1"></a>`signature`: `"\"(cls, value: 'tuple[SamplerOutput, ...]') -> 'tuple[SamplerOutput, ...]'\""`

## Maintained corroboration

### Related interface records

- [SamplerResultPayload](stove0-review-sampler-protocol-samplerresultpayload.md)

## Governing policies

- <a id="pa-79daebb013"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources/authorities.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerResultPayload.canonical_outputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b29446018cf219ac8e5ab6dd21a15c61888493d2f770993972fc50ad2d230f91 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SamplerOutput, ...]') -> 'tuple[SamplerOutput, ...]'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "canonical_outputs",
  "owner": "stove0_review_sampler_protocol.SamplerResultPayload",
  "unit": "member"
}
```

</details>
