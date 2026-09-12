# schemas: ExternalEffectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-externaleffectreceipt:ceb57cc4bd -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 9 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ExternalEffectReceipt`

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
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| encoded-size | bytes | `contract_max` | maximum=65536, reason=bounded-external-effect-receipt |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: ExternalEffectReceipt
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `execution_sha256` | yes | string |  |
| `format` | no | string |  |
| `job_id` | yes | string |  |
| `operation_contract_sha256` | yes | string |  |
| `plan_sha256` | yes | string |  |
| `receipt_sha256` | yes | string |  |
| `request_sha256` | yes | string |  |
| `result` | yes | object |  |
| `target_contract_sha256` | yes | string |  |
