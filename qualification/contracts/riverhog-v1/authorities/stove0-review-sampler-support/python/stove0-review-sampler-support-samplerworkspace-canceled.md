# stove0_review_sampler_support.SamplerWorkspace.canceled

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-samplerwork-409939d6b1:06427496f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-47bab610a1"></a>
| Field | Shape |
|---|---|
| <a id="s-5ba5060d3c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-82b39c1239"></a>`distribution` | "stove0-review-sampler-support" |
| <a id="s-0edc39e96d"></a>`module` | "stove0_review_sampler_support" |
| <a id="s-50d64612db"></a>`name` | "canceled" |
| <a id="s-8c2b4a5797"></a>`owner` | "stove0_review_sampler_support.SamplerWorkspace" |
| <a id="s-c01824ac46"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_support.SamplerWorkspace](stove0-review-sampler-support-samplerworkspace.md)

## Governing policies

- <a id="pa-8c143c8b73"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.SamplerWorkspace.canceled`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c752a853d381517e0b3454fbd9e5b89ed8d31d1a141affd4fd78b00144dca56 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bool'\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "canceled",
  "owner": "stove0_review_sampler_support.SamplerWorkspace",
  "unit": "member"
}
```
