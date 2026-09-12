# schemas: TargetProgress

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetprogress:06cefed426 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: TargetProgress
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `completed` | yes | type="integer"; minimum=0 |  |
| `phase` | yes | type="string"; minLength=1; maxLength=120 |  |
| `total` | no | anyOf=type="integer"; minimum=0 \| type="null" |  |
| `unit` | no | anyOf=type="string"; minLength=1; maxLength=40 \| type="null" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=40, minimum=1, reason=schema-maximum |

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetProgress`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39df6fbc583523c116848af049b9e80abb672fa8af47887fcd5f7a791684498a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "completed": {
      "minimum": 0,
      "title": "Completed",
      "type": "integer"
    },
    "phase": {
      "maxLength": 120,
      "minLength": 1,
      "title": "Phase",
      "type": "string"
    },
    "total": {
      "anyOf": [
        {
          "minimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Total"
    },
    "unit": {
      "anyOf": [
        {
          "maxLength": 40,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Unit"
    }
  },
  "required": [
    "phase",
    "completed"
  ],
  "title": "TargetProgress",
  "type": "object"
}
```
