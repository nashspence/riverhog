# schemas: RiverhogEventCause

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-riverhogeventcause:59b3f4a517 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RiverhogEventCause`

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
| length | characters | `contract_max` | maximum=300, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=300, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: RiverhogEventCause
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `id` | yes | string |  |
| `source` | yes | string |  |
| `subject` | no | object (2 fields) |  |
| `type` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac29075e27ddb589a896c56213e369861c512dd5f6d747bf10c49fe01431fcb2 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "id": {
      "maxLength": 300,
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "source": {
      "maxLength": 1000,
      "minLength": 1,
      "title": "Source",
      "type": "string"
    },
    "subject": {
      "anyOf": [
        {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Subject"
    },
    "type": {
      "maxLength": 300,
      "minLength": 1,
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "source",
    "type"
  ],
  "title": "RiverhogEventCause",
  "type": "object"
}
```
