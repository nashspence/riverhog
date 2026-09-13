# stove0_review_target_support.ReviewTargetServiceBase.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-reviewtarget-70f75d6349:60ac3df4f8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aaf1d08502"></a>
| Field | Shape |
|---|---|
| <a id="s-fa07f7e7cd"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-756689318f"></a>`distribution` | "stove0-review-target-support" |
| <a id="s-7210cd947c"></a>`module` | "stove0_review_target_support" |
| <a id="s-cb7f3800fd"></a>`name` | "preflight" |
| <a id="s-29c36810e0"></a>`owner` | "stove0_review_target_support.ReviewTargetServiceBase" |
| <a id="s-54d0b5fce5"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_review_target_support.ReviewTargetServiceBase](stove0-review-target-support-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-cad86c335c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.ReviewTargetServiceBase.preflight`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b84ddc1e60cab222d109432a58eddaeb874f11c4eadf7d5d157d2defbca397a7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "preflight",
  "owner": "stove0_review_target_support.ReviewTargetServiceBase",
  "unit": "member"
}
```
