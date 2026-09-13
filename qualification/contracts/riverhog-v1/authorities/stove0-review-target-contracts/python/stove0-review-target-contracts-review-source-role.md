# stove0_review_target_contracts.REVIEW_SOURCE_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-review-source-role:8df7156ce9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a2accdbd33"></a>
| Field | Shape |
|---|---|
| <a id="s-7644615495"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-f5ebb2e439"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-cc9c888e5f"></a>`module` | "stove0_review_target_contracts" |
| <a id="s-9d7559fcd3"></a>`name` | "REVIEW_SOURCE_ROLE" |
| <a id="s-0fa1d8e488"></a>`unit` | "export" |

## Governing policies

- <a id="pa-2318d1db85"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.REVIEW_SOURCE_ROLE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 249e6adea7164f40f7e3423ac0e0abff7397ab6c582e6e2a36bb963c5ad2de43 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.source/v1"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "REVIEW_SOURCE_ROLE",
  "unit": "export"
}
```
