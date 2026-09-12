# schemas: BranchSetEvaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchsetevaluation:7f019b5895 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 11 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSetEvaluation`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

- `title`: BranchSetEvaluation
- `description`: Entirely derived view over a plan and ordinary child/join results.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `branch_set_sha256` | yes | string |  |
| `branch_set_succeeded` | yes | boolean |  |
| `canceled_branch_ids` | yes | array |  |
| `coordination_complete_for_retirement` | yes | boolean |  |
| `coordination_settlement` | yes | object (1 fields) |  |
| `failed_branch_ids` | yes | array |  |
| `inapplicable_branch_ids` | yes | array |  |
| `interrupted_branch_ids` | yes | array |  |
| `join_ready` | yes | boolean |  |
| `join_settlement` | yes | object (1 fields) |  |
| `join_state` | yes | string |  |
| `resolved_join_plan` | yes | object (1 fields) |  |
| `retirement_requested` | yes | boolean |  |
| `succeeded_branches` | yes | array |  |
| `succeeded_coordinations` | yes | array |  |
| `succeeded_effects` | yes | array |  |
| `unsettled_branch_ids` | yes | array |  |
| `unsettled_work_ids` | yes | array |  |
