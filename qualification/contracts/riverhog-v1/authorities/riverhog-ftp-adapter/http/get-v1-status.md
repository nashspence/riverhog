# GET /v1/status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog-ftp-adapter:get-v1-status:315efcb5c6 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `http` |
| Family | `status` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/paths/~1v1~1status/get`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog-ftp-adapter` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: get_ftp_adapter_status](../operation/operation-parity-get-ftp-adapter-status.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| logical-result-cardinality | items | `segmented_no_total_max` | reason=bounded-route-progression |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

- `operationId`: get_ftp_adapter_status
- `summary`: Status
- `security`: `[{"RiverhogFtpAdapterBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `page_size` | query | no | integer |
| `page_token` | query | no | object (2 fields) |

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `500` | Internal Server Error |
