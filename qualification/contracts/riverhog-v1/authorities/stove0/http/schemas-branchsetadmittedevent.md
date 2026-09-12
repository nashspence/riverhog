# schemas: BranchSetAdmittedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchsetadmittedevent:23dd1d777b -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSetAdmittedEvent`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: BranchSetAdmittedEvent
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `data` | yes | #/components/schemas/BranchSetAdmittedEventData |  |
| `datacontenttype` | no | string |  |
| `id` | yes | string |  |
| `source` | yes | string |  |
| `specversion` | no | string |  |
| `subject` | yes | string |  |
| `time` | yes | string |  |
| `type` | yes | string |  |
