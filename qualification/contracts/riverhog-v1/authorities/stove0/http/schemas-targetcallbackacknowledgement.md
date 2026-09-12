# schemas: TargetCallbackAcknowledgement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetcallbackacknowledgement:6df5ac040c -->

Idempotent acceptance of one execution-scoped declaration.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: TargetCallbackAcknowledgement
- `description`: Idempotent acceptance of one execution-scoped declaration.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `accepted` | no | type="boolean"; const=true |  |

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetCallbackAcknowledgement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 888aeb6259ebbfb946fb389f43530f2e7f42b0aaa47127fc93cde9b46c06ccfc -->

```json
{
  "additionalProperties": false,
  "description": "Idempotent acceptance of one execution-scoped declaration.",
  "properties": {
    "accepted": {
      "const": true,
      "default": true,
      "title": "Accepted",
      "type": "boolean"
    }
  },
  "title": "TargetCallbackAcknowledgement",
  "type": "object"
}
```
