# schemas: CollectionUploadUnitAssignmentDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadunitassignmentdocument:3b1d8dc849 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadUnitAssignmentDocument`

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

- `title`: CollectionUploadUnitAssignmentDocument
- `description`: One bounded, immutable unit offered by an exact upload session.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `plan_sha256` | yes | string |  |
| `unit` | yes | #/components/schemas/CollectionUploadUnitWorkDocument |  |
| `volume` | yes | #/components/schemas/CollectionUploadVolumeSummaryDocument |  |
