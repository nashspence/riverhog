# schemas: WorkView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workview:754d420c65 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkView`

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
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: WorkView
- `description`: Operator projection of mutable work; never an execution identity.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `abandon_outcome` | no | object (2 fields) |  |
| `branch_set_plan` | no | object (1 fields) |  |
| `claim` | no | object (1 fields) |  |
| `controller_evidence` | no | object (1 fields) |  |
| `coordination_cancel_requested` | no | boolean |  |
| `coordination_settlement` | no | object (1 fields) |  |
| `expected_target_plan_sha256` | no | object (2 fields) |  |
| `failure` | no | object (1 fields) |  |
| `format` | no | string |  |
| `inapplicable` | no | object (1 fields) |  |
| `join_plan` | no | object (1 fields) |  |
| `observation_requests` | no | array |  |
| `observation_results` | no | array |  |
| `output` | no | object (1 fields) |  |
| `phase` | yes | string |  |
| `preview_acceptance` | no | object (1 fields) |  |
| `retirement_remaining` | no | array |  |
| `revision` | yes | integer |  |
| `target_plan` | no | object (2 fields) |  |
| `target_request` | no | object (1 fields) |  |
| `target_settlement` | no | object (1 fields) |  |
| `target_status` | no | object (1 fields) |  |
| `work` | yes | #/components/schemas/WorkIdentity |  |
| `work_id` | yes | string |  |
| `workflow_plan` | no | object (1 fields) |  |
