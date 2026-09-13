# stove0_review_target_support.ReviewTargetConfig.canonical_samplers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-reviewtarget-eeaae60201:27476f423f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2c808bf02f"></a>
| Field | Shape |
|---|---|
| <a id="s-12f72c3921"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-8918c52bba"></a>`distribution` | "stove0-review-target-support" |
| <a id="s-accf4ec019"></a>`module` | "stove0_review_target_support" |
| <a id="s-88d4dbb43c"></a>`name` | "canonical_samplers" |
| <a id="s-38369c8395"></a>`owner` | "stove0_review_target_support.ReviewTargetConfig" |
| <a id="s-4c0029f239"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_review_target_support.ReviewTargetConfig](stove0-review-target-support-reviewtargetconfig.md)

## Governing policies

- <a id="pa-0eb1f42815"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.ReviewTargetConfig.canonical_samplers`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bdd77414f151e930cb785e906faba79ee499c2a969d1dea1a7017b99e0218f8a -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SamplerConfig, ...]') -> 'tuple[SamplerConfig, ...]'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "canonical_samplers",
  "owner": "stove0_review_target_support.ReviewTargetConfig",
  "unit": "member"
}
```
