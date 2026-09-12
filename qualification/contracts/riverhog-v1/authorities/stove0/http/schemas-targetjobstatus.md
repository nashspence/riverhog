# schemas: TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetjobstatus:5145d7a958 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetJobStatus`

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
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: TargetJobStatus
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `attempt` | yes | integer |  |
| `derivation` | no | object (2 fields) |  |
| `effect_receipt` | no | object (1 fields) |  |
| `execution_evidence` | no | object (1 fields) |  |
| `failure` | no | object (1 fields) |  |
| `inapplicable` | no | object (1 fields) |  |
| `job_id` | yes | string |  |
| `output_collection` | no | object (1 fields) |  |
| `plan_sha256` | yes | string |  |
| `production` | no | object (1 fields) |  |
| `progress` | yes | #/components/schemas/TargetProgress |  |
| `protocol` | no | string |  |
| `request_sha256` | yes | string |  |
| `state` | yes | string |  |
