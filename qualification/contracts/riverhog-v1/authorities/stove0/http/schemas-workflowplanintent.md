# schemas: WorkflowPlanIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workflowplanintent:747bd5d627 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkflowPlanIntent`

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
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: WorkflowPlanIntent
- `description`: Work-independent fields that deterministically materialize a workflow plan.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `input_retrieval_policy` | no | string |  |
| `operation` | yes | #/components/schemas/OperationRef |  |
| `output_policy` | no | object |  |
| `requested_target_options` | no | object |  |
| `result_kind` | no | string |  |
| `retirement_grace_seconds` | no | integer |  |
| `retirement_policy` | no | string |  |
| `target_contract_sha256` | yes | string |  |
| `target_registration_id` | yes | string |  |
