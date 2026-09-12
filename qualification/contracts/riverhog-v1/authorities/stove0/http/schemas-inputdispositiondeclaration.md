# schemas: InputDispositionDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-inputdispositiondeclaration:3a76d71343 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/InputDispositionDeclaration`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- `title`: InputDispositionDeclaration
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `input_id` | yes | string |  |
| `status` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e94d6588a913858886bdbd76c5ea8b089aeca0bc91025c491822f3a23edf6dba -->

```json
{
  "additionalProperties": false,
  "properties": {
    "input_id": {
      "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
      "title": "Input Id",
      "type": "string"
    },
    "status": {
      "enum": [
        "transformed",
        "preserved",
        "omitted",
        "rejected"
      ],
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "input_id",
    "status"
  ],
  "title": "InputDispositionDeclaration",
  "type": "object"
}
```
