# POST /v1/admission-policies/{policy_id}:rebaseline

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:post-v1-admission-policies-policy-id-rebaseline:174f746dd2 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `admission-policies` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1admission-policies~1{policy_id}:rebaseline/post`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: rebaseline_admission_policy](../operation/operation-parity-rebaseline-admission-policy.md)

## Contract

- `operationId`: rebaseline_admission_policy
- `summary`: Rebaseline Admission Policy

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `policy_id` | path | yes | string |

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `500` | Internal Server Error |
