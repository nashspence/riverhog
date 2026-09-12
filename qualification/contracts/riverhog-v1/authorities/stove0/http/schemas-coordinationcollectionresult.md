# schemas: CoordinationCollectionResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-coordinationcollectionresult:63aad8c924 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CoordinationCollectionResult`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: CoordinationCollectionResult
- `description`: Parent-visible collection produced by the coordinator's actual join leaf.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `derivation_sha256` | yes | string |  |
| `join_settlement_sha256` | yes | string |  |
| `output_collection` | yes | #/components/schemas/CollectionRootRef |  |
| `output_selection` | yes | #/components/schemas/ArtifactSelectionRef |  |
| `producer_work_id` | yes | string |  |
