# schemas: AdmissionPolicyStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionpolicystatus:f36d63eb97 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionPolicyStatus`

## Effective policies

- `compatibility/http-api/v1`
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
| length | characters | `contract_max` | maximum=40, minimum=1, reason=schema-maximum |

## Contract

- `title`: AdmissionPolicyStatus
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `authorization_view_identity` | no | object (2 fields) |  |
| `baseline_mode` | yes | string |  |
| `phase` | yes | string |  |
| `policy` | yes | #/components/schemas/AdmissionPolicy |  |
| `policy_sha256` | yes | string |  |
| `source_identity` | no | object (2 fields) |  |
| `through_revision` | yes | string |  |
| `updated_at` | yes | string |  |
