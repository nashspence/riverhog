# schemas: SchedulerRun

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-schedulerrun:389a6dbea7 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerRun`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: AdmissionRun](schemas-admissionrun.md)
- [schemas: SchedulerPruning](schemas-schedulerpruning.md)
- [schemas: SchedulerWorkBatch](schemas-schedulerworkbatch.md)

## Contract summary

- `title`: SchedulerRun
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `admission` | no | object (1 fields) |  |
| `pruning` | yes | object (1 fields) |  |
| `work` | yes | #/components/schemas/SchedulerWorkBatch |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 18340e39255b5eeffda73125a891665cd47056ed52aae5e771c92c75a8d1cac8 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "admission": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/AdmissionRun"
        },
        {
          "type": "null"
        }
      ]
    },
    "pruning": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/SchedulerPruning"
        },
        {
          "type": "null"
        }
      ]
    },
    "work": {
      "$ref": "#/components/schemas/SchedulerWorkBatch"
    }
  },
  "required": [
    "pruning",
    "work"
  ],
  "title": "SchedulerRun",
  "type": "object"
}
```
