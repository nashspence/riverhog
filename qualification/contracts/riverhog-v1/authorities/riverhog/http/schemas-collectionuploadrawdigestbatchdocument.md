# schemas: CollectionUploadRawDigestBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadrawdigestbatchdocument:088760615c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadRawDigestBatchDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`
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
| cardinality | items | `segmented_no_total_max` | maximum=1024, minimum=1, reason=bounded-raw-digest-append |

## Contract

- `title`: CollectionUploadRawDigestBatchDocument
- `description`: One append-only bounded slice of a registered raw source digest sequence.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `first_part` | yes | integer |  |
| `path` | yes | string |  |
| `sha256s` | yes | array |  |
