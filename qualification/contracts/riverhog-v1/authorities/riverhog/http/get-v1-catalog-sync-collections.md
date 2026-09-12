# GET /v1/catalog-sync/collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-catalog-sync-collections:1a92a5f165 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `catalog-sync` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1catalog-sync~1collections/get`

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

- [Operation parity: list_catalog_sync_collections](../operation/operation-parity-list-catalog-sync-collections.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| logical-result-cardinality | items | `segmented_no_total_max` | reason=bounded-route-progression |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract

- `operationId`: list_catalog_sync_collections
- `summary`: List Catalog Sync Collections
- `security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `cursor` | query | yes | string |
| `limit` | query | no | integer |

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `409` | Conflict |
| `410` | Gone |
| `500` | Internal Server Error |
