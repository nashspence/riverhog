# schemas: PortableCollectionHeader

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-portablecollectionheader:c3da8f056c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PortableCollectionHeader`

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
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: PortableCollectionHeader
- `description`: Bounded immutable metadata that owns one portable file inventory.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection` | yes | #/components/schemas/CollectionId |  |
| `content_identity` | yes | string |  |
| `encryption_format` | yes | string |  |
| `format` | no | string |  |
| `passphrase_id` | yes | string |  |
| `provenance_identity` | no | object (2 fields) |  |
| `provenance_mode` | yes | string |  |
