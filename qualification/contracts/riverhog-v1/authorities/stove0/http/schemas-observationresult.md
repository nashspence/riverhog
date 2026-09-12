# schemas: ObservationResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-observationresult:732d745985 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 7 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObservationResult`

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
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

- `title`: ObservationResult
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `execution_evidence` | no | object |  |
| `facts` | no | object (2 fields) |  |
| `facts_schema` | no | object (1 fields) |  |
| `facts_sha256` | no | object (2 fields) |  |
| `failure` | no | object (1 fields) |  |
| `format` | no | string |  |
| `inapplicable` | no | object (1 fields) |  |
| `observer` | yes | #/components/schemas/ObserverImplementation |  |
| `observer_contract_id` | yes | string |  |
| `observer_contract_sha256` | yes | string |  |
| `request_id` | yes | string |  |
| `result_sha256` | yes | string |  |
| `state` | yes | string |  |
| `subjects` | yes | array |  |
