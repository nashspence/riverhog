# stove0_review_sampler_support.SamplerClient.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-samplerclie-7ff86022e6:f4d21a1154 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5aa5936584"></a>
| Field | Shape |
|---|---|
| <a id="s-1cf4ea9c1f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-bb28d95afe"></a>`distribution` | "stove0-review-sampler-support" |
| <a id="s-8be6cec807"></a>`module` | "stove0_review_sampler_support" |
| <a id="s-4b51e64562"></a>`name` | "descriptor" |
| <a id="s-8cebf7ec48"></a>`owner` | "stove0_review_sampler_support.SamplerClient" |
| <a id="s-8539c9cb23"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_support.SamplerClient](stove0-review-sampler-support-samplerclient.md)

## Governing policies

- <a id="pa-c62b8a009a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.SamplerClient.descriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 10824f88ca6f62805170f38b57916ee3eeb0622b426176eeafeb063a392a7449 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, refresh: 'bool' = False) -> 'SamplerDescriptor'\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "descriptor",
  "owner": "stove0_review_sampler_support.SamplerClient",
  "unit": "member"
}
```
