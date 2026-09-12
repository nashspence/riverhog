# schemas: PortableCollectionInventoryPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-portablecollectioninventorypage:e52591cde2 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PortableCollectionInventoryPage`

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
| cardinality | items | `segmented_no_total_max` | maximum=1000, reason=bounded-route-page |
| length | characters | `contract_max` | maximum=8192, minimum=1, reason=schema-maximum |

## Contract

- `title`: PortableCollectionInventoryPage
- `description`: One bounded, canonically ordered slice of an immutable inventory.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `authority` | yes | #/components/schemas/PortableCollectionInventoryAuthority |  |
| `complete` | yes | boolean |  |
| `files` | yes | array |  |
| `format` | no | string |  |
| `next_cursor` | no | object (2 fields) |  |
