# stove0_review_target_contracts.ReviewSamplePlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-reviewsampleplan:bcbc679873 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c6cdc8f921"></a>
| Field | Shape |
|---|---|
| <a id="s-499f6ff698"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-21422ef3c8"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-9bc19bffa7"></a>`module` | "stove0_review_target_contracts" |
| <a id="s-df89e02872"></a>`name` | "ReviewSamplePlan" |
| <a id="s-f232ea82b5"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_target_contracts.ReviewSamplePlan.verify_digest](stove0-review-target-contracts-reviewsampleplan-verify-digest.md)
- [stove0_review_target_contracts.ReviewSamplePlan.seal](stove0-review-target-contracts-reviewsampleplan-seal.md)

## Governing policies

- <a id="pa-f47cf88d91"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.ReviewSamplePlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c789879197742e4ebd6e3caa21f7adfed9cda8902649af633e36ecf0d3408e6d -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "300236b15485789a0e69f7afea9ac0bc549d8af384a88a3d5bf91dcd765ddd84",
    "signature": "\"(*, format: Literal['stove0-review-sample-plan/v1'] = 'stove0-review-sample-plan/v1', selection_method: Literal['evenly-spaced/v1'] = 'evenly-spaced/v1', samples_per_artifact: Annotated[int, Ge(ge=1)], window_duration_ms: Annotated[int, Ge(ge=1)], windows: Annotated[tuple[stove0_review_target_contracts.models.ReviewSampleWindow, ...], MinLen(min_length=1)], sample_plan_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "ReviewSamplePlan",
  "unit": "export"
}
```
