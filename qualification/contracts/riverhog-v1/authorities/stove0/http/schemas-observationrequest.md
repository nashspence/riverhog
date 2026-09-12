# schemas: ObservationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-observationrequest:a5956711b0 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 8 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObservationRequest`

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
| value | schema-value | `contract_max` | maximum=67108864, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=86400, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: ObservationRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `format` | no | string |  |
| `maximum_result_bytes` | no | integer |  |
| `observer_contract_id` | yes | string |  |
| `observer_contract_sha256` | yes | string |  |
| `observer_descriptor_sha256` | yes | string |  |
| `observer_registration_id` | yes | string |  |
| `options` | no | object |  |
| `request_id` | yes | string |  |
| `retrieval_policy` | no | string |  |
| `subjects` | yes | array |  |
| `timeout_seconds` | no | integer |  |
| `work_id` | yes | string |  |
