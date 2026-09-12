# GET /v1/catalog/collections/{collection_id}/inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-catalog-collections-collection-id-inventory:fffa90f23d -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `catalog` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1catalog~1collections~1{collection_id}~1inventory/get`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: get_portable_collection_inventory](../operation/operation-parity-get-portable-collection-inventory.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| logical-result-cardinality | items | `segmented_no_total_max` | reason=bounded-route-progression |
| length | characters | `contract_max` | maximum=8192, minimum=1, reason=schema-maximum |
| value | schema-value | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |

## Contract

- `operationId`: get_portable_collection_inventory
- `summary`: Get Portable Collection Inventory
- `security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `collection_id` | path | yes | integer |
| `cursor` | query | no | object (2 fields) |
| `limit` | query | no | integer |
| `If-Match` | header | no | object (2 fields) |

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `404` | Not Found |
| `412` | Precondition Failed |
| `428` | Precondition Required |
| `500` | Internal Server Error |
