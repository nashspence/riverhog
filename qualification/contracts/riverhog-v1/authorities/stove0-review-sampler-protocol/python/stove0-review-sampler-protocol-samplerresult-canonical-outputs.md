# stove0_review_sampler_protocol.SamplerResult.canonical_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerres-027070fb4b:949f23397b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-09748d01d0"></a>
- <a id="s-6e3c70d12e"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-2f67984017"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-c5acf4edcd"></a>`name`: `canonical_outputs`
- <a id="s-8453a42639"></a>`owner`: `stove0_review_sampler_protocol.SamplerResult`
- <a id="s-72f9f7654d"></a>`unit`: `member`

### Declared structure

- <a id="s-18dd8fe9a1"></a>`kind`: `"classmethod"`
- <a id="s-343eb3ebd6"></a>`signature`: `"\"(cls, value: 'tuple[SamplerOutput, ...]') -> 'tuple[SamplerOutput, ...]'\""`

## Maintained corroboration

### Related interface records

- [SamplerResult](stove0-review-sampler-protocol-samplerresult.md)

## Governing policies

- <a id="pa-e5c1efc2c6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources/authorities.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerResult.canonical_outputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a1e438e0b65db1798b4815dd455a44a17092e7662dff79e7e74a8a956b23741d -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SamplerOutput, ...]') -> 'tuple[SamplerOutput, ...]'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "canonical_outputs",
  "owner": "stove0_review_sampler_protocol.SamplerResult",
  "unit": "member"
}
```

</details>
