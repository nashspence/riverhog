# stove0_review_target_contracts.ReviewSamplePlan.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-reviewsampleplan-seal:05c66328d7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cce4ed2455"></a>
- <a id="s-e8f5cd88ba"></a>`distribution`: `stove0-review-target-contracts`
- <a id="s-c4f4d6f412"></a>`module`: `stove0_review_target_contracts`
- <a id="s-ddc254ff52"></a>`name`: `seal`
- <a id="s-bcb48aab49"></a>`owner`: `stove0_review_target_contracts.ReviewSamplePlan`
- <a id="s-b6a9127d32"></a>`unit`: `member`

### Declared structure

- <a id="s-b5eae63e0d"></a>`kind`: `"classmethod"`
- <a id="s-5524125603"></a>`signature`: `"\"(cls, payload: 'ReviewSamplePlanPayload') -> 'ReviewSamplePlan'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplePlan](stove0-review-target-contracts-reviewsampleplan.md)

## Governing policies

- <a id="pa-2fd92ac8e9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.ReviewSamplePlan.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b21a250e7cc53242da6e92f685a670d2cc3979501ebe6dae546202a6e988a9c -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'ReviewSamplePlanPayload') -> 'ReviewSamplePlan'\""
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "seal",
  "owner": "stove0_review_target_contracts.ReviewSamplePlan",
  "unit": "member"
}
```
