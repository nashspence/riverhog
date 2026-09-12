# schemas: ErrorBody

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-errorbody:dccf3fddd8 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ErrorBody`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: ErrorBody
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `code` | yes | string |  |
| `details` | no | object (3 fields) |  |
| `message` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6760c34bd592b6639dc349282b6e02f6cebfa1985aa1a5b2c1943c0faa367741 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "code": {
      "minLength": 1,
      "title": "Code",
      "type": "string"
    },
    "details": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Details"
    },
    "message": {
      "minLength": 1,
      "title": "Message",
      "type": "string"
    }
  },
  "required": [
    "code",
    "message"
  ],
  "title": "ErrorBody",
  "type": "object"
}
```
