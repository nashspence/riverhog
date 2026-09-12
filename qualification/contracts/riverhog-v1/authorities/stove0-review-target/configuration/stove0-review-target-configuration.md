# stove0-review-target configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-review-target:stove0-review-target-configuration:39ebbd838e -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-target` |
| Interface | `configuration` |
| Family | `documents` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/configuration_documents/stove0-review-target`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configuration-composition/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `configuration:stove0-review-target` — `reference/stove0/targets/review/support/src/stove0_review_target_support/app.py::ReviewTargetConfig`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=2048, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |

## Contract

- `title`: ReviewTargetConfig
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `samplers` | yes | array |  |

### Definitions

| Definition | Shape |
|---|---|
| `SamplerConfig` | object |
