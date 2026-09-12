# schemas: OutputSourceEdge

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-outputsourceedge:5e885e28fd -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/OutputSourceEdge`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- `title`: OutputSourceEdge
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `input_id` | yes | string |  |
| `output_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 972b082324ce99560dcf290aaa2190dca0d8d4ad38ed3ae820655588e94db2f4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "input_id": {
      "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
      "title": "Input Id",
      "type": "string"
    },
    "output_id": {
      "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
      "title": "Output Id",
      "type": "string"
    }
  },
  "required": [
    "output_id",
    "input_id"
  ],
  "title": "OutputSourceEdge",
  "type": "object"
}
```
