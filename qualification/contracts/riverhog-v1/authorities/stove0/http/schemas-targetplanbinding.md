# schemas: TargetPlanBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetplanbinding:97956d1dde -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetPlanBinding`

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
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: TargetPlanBinding
- `description`: Opaque binding to a target-owned preflight plan.  The target protocol owns the plan schema and canonicalization algorithm. stove0 retains the complete validated plan document and its target-issued digest, but deliberately does not reinterpret or re-hash the plan with stove0's canonical JSON rules. This prevents two authorities from disagreeing about target plan identity while preserving the full document in the execution envelope.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `operation_contract_sha256` | yes | string |  |
| `plan` | yes | object |  |
| `plan_sha256` | yes | string |  |
| `protocol` | yes | string |  |
| `target_contract_sha256` | yes | string |  |
| `target_implementation_id` | yes | string |  |
