# schemas: PortableCollectionInventoryAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-portablecollectioninventoryauthority:df9243ffe3 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PortableCollectionInventoryAuthority`

## Effective policies

- `compatibility/http-api/v1`
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

## Contract

- `title`: PortableCollectionInventoryAuthority
- `description`: The immutable authority shared by every bounded inventory page.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `file_bytes` | yes | integer |  |
| `file_count` | yes | integer |  |
| `header` | yes | #/components/schemas/PortableCollectionHeader |  |
| `inventory_identity` | yes | string |  |
