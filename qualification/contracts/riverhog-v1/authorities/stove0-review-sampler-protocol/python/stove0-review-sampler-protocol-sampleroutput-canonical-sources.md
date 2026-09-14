# stove0_review_sampler_protocol.SamplerOutput.canonical_sources

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerout-3235b4aab5:9eae1c325a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1770660631"></a>
- <a id="s-62eff33c8b"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-559f5c8569"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-a113bb723b"></a>`name`: `canonical_sources`
- <a id="s-1255e0753f"></a>`owner`: `stove0_review_sampler_protocol.SamplerOutput`
- <a id="s-7fdddebca1"></a>`unit`: `member`

### Declared structure

- <a id="s-8358f81552"></a>`kind`: `"classmethod"`
- <a id="s-c100f6df42"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [SamplerOutput](stove0-review-sampler-protocol-sampleroutput.md)

## Governing policies

- <a id="pa-8591f58ec4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerOutput.canonical_sources`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5522259d1c0f7c1d4fb93561c0959d918ccede74e78178b923c6fa1fd243b982 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "canonical_sources",
  "owner": "stove0_review_sampler_protocol.SamplerOutput",
  "unit": "member"
}
```
