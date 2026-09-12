# schemas: Stove0LifecycleEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-stove0lifecycleevent:31fdc50ad8 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/Stove0LifecycleEvent`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

```json
{
  "discriminator": {
    "mapping": {
      "io.riverhog.stove0.branch-set.admitted": "#/components/schemas/BranchSetAdmittedEvent",
      "io.riverhog.stove0.evaluation.created": "#/components/schemas/EvaluationCreatedEvent",
      "io.riverhog.stove0.evaluation.updated": "#/components/schemas/EvaluationUpdatedEvent",
      "io.riverhog.stove0.join.admitted": "#/components/schemas/JoinAdmittedEvent",
      "io.riverhog.stove0.work.created": "#/components/schemas/WorkCreatedEvent",
      "io.riverhog.stove0.work.updated": "#/components/schemas/WorkUpdatedEvent"
    },
    "propertyName": "type"
  },
  "oneOf": [
    {
      "$ref": "#/components/schemas/WorkCreatedEvent"
    },
    {
      "$ref": "#/components/schemas/WorkUpdatedEvent"
    },
    {
      "$ref": "#/components/schemas/BranchSetAdmittedEvent"
    },
    {
      "$ref": "#/components/schemas/JoinAdmittedEvent"
    },
    {
      "$ref": "#/components/schemas/EvaluationCreatedEvent"
    },
    {
      "$ref": "#/components/schemas/EvaluationUpdatedEvent"
    }
  ]
}
```
