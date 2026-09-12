# schemas: CatalogSyncChangePage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-catalogsyncchangepage:a10a7f5278 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CatalogSyncChangePage`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `segmented_no_total_max` | maximum=100, reason=bounded-route-page |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=19, minimum=1, reason=schema-maximum |

## Contract

- `title`: CatalogSyncChangePage
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `authorization_view_identity` | yes | string |  |
| `caught_up` | yes | boolean |  |
| `changes` | yes | array |  |
| `format` | no | string |  |
| `next_cursor` | yes | string |  |
| `source_identity` | yes | string |  |
| `through_revision` | yes | string |  |
