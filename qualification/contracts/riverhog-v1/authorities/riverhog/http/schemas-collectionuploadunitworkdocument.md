# schemas: CollectionUploadUnitWorkDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadunitworkdocument:97931cc282 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadUnitWorkDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=1000, minimum=None, reason=bounded-upload-unit-source-map |

## Contract

- `title`: CollectionUploadUnitWorkDocument
- `description`: One exact unit and its durable upload checkpoint state.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `payload_bytes` | yes | integer |  |
| `plaintext_bytes` | yes | integer |  |
| `sources` | yes | array |  |
| `state` | yes | string |  |
| `unit` | yes | integer |  |
