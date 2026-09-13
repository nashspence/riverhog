# stove0_review_target_contracts.ReviewSamplePlanPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-reviewsamp-e2c24613bd:6cabb8cdb5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c26a2b702f"></a>
| Field | Shape |
|---|---|
| <a id="s-b45f7e3474"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7a724c46a7"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-adf911076e"></a>`module` | "stove0_review_target_contracts" |
| <a id="s-9e73fecc70"></a>`name` | "ReviewSamplePlanPayload" |
| <a id="s-e478ca86a6"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_target_contracts.ReviewSamplePlanPayload.canonical_windows](stove0-review-target-contracts-reviewsampleplanpayload-canonical-windows.md)
- [stove0_review_target_contracts.ReviewSamplePlanPayload.exact_declared_shape](stove0-review-target-contracts-reviewsampleplanpayload-exact-declared-shape.md)

## Governing policies

- <a id="pa-455b87c5e5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.ReviewSamplePlanPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a879118c87e0030598d7f36fae5762945d6c13378856ab4231ec50fb30abc9a -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d44be71fa4e1e60918c17a8a6f73a93632ad718254b63595c6484604c69262f2",
    "signature": "\"(*, format: Literal['stove0-review-sample-plan/v1'] = 'stove0-review-sample-plan/v1', selection_method: Literal['evenly-spaced/v1'] = 'evenly-spaced/v1', samples_per_artifact: Annotated[int, Ge(ge=1)], window_duration_ms: Annotated[int, Ge(ge=1)], windows: Annotated[tuple[stove0_review_target_contracts.models.ReviewSampleWindow, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "ReviewSamplePlanPayload",
  "unit": "export"
}
```
