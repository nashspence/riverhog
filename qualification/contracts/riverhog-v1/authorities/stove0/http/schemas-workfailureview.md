# schemas: WorkFailureView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workfailureview:2dbb61c015 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkFailureView`

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
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: WorkFailureView
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `code` | yes | string |  |
| `message` | yes | string |  |
| `retryable` | yes | boolean |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a61374424ae470a2c694ac13502faf98f91af05992638c02e1d5b5380dac0c4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "code": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Code",
      "type": "string"
    },
    "message": {
      "maxLength": 1000,
      "minLength": 1,
      "title": "Message",
      "type": "string"
    },
    "retryable": {
      "title": "Retryable",
      "type": "boolean"
    }
  },
  "required": [
    "code",
    "message",
    "retryable"
  ],
  "title": "WorkFailureView",
  "type": "object"
}
```
