# schemas: HealthResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-healthresponse:58e5097207 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/HealthResponse`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- `title`: HealthResponse
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `service` | yes | string |  |
| `status` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 873f58b65973a85d82bd4e352acd595a8f32f6058c4500f11514358669b42b31 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "service": {
      "minLength": 1,
      "title": "Service",
      "type": "string"
    },
    "status": {
      "const": "ok",
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "service",
    "status"
  ],
  "title": "HealthResponse",
  "type": "object"
}
```
