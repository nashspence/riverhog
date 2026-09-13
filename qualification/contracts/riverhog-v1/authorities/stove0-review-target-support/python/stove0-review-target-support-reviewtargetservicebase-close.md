# stove0_review_target_support.ReviewTargetServiceBase.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-reviewtarget-cec516d58c:59c8f00cf5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ba62aef2d0"></a>
| Field | Shape |
|---|---|
| <a id="s-7d844ab870"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a3e8a78853"></a>`distribution` | "stove0-review-target-support" |
| <a id="s-6c15fb4fed"></a>`module` | "stove0_review_target_support" |
| <a id="s-17b5463742"></a>`name` | "close" |
| <a id="s-4999239819"></a>`owner` | "stove0_review_target_support.ReviewTargetServiceBase" |
| <a id="s-1390180595"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_review_target_support.ReviewTargetServiceBase](stove0-review-target-support-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-6824ac5de1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.ReviewTargetServiceBase.close`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8ee383257309bb6a508a3a0f352ab75099238581cbe853ab6e00e36dcc34f59 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "close",
  "owner": "stove0_review_target_support.ReviewTargetServiceBase",
  "unit": "member"
}
```
