# schemas: RecipeDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-recipedefinition:31a48bb826 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeDefinition`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

- `title`: RecipeDefinition
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `allow_derived_inputs` | no | boolean |  |
| `artifact_associations` | no | array |  |
| `event_input_closure` | no | string |  |
| `id` | yes | string |  |
| `join` | no | object (1 fields) |  |
| `observers` | no | array |  |
| `retirement_grace_seconds` | no | integer |  |
| `revision` | yes | integer |  |
| `routes` | yes | array |  |
| `source_retirement_policy` | no | string |  |
| `unmatched_artifact_disposition` | yes | string |  |
