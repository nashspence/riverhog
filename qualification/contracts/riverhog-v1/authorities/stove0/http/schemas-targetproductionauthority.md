# schemas: TargetProductionAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetproductionauthority:5d406c80aa -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetProductionAuthority`

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
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: TargetProductionAuthority
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `disposition_count` | yes | integer |  |
| `disposition_sha256` | yes | string |  |
| `format` | no | string |  |
| `job_id` | yes | string |  |
| `outputs` | yes | #/components/schemas/OutputArtifactSetIdentity |  |
| `plan_sha256` | yes | string |  |
| `production_sha256` | yes | string |  |
| `riverhog_disposition_set` | yes | #/components/schemas/ArtifactDispositionSetIdentity |  |
| `source_edge_count` | yes | integer |  |
| `source_edge_sha256` | yes | string |  |
