# schemas: RiverhogActor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-riverhogactor:77ecdb52af -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RiverhogActor`

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
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=300, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: RiverhogActor
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `app` | yes | string |  |
| `key_id` | no | object (2 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae88be9c9fa5a2e60977b689011476674a5c673474d5e841bf78ebb369101905 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "app": {
      "maxLength": 160,
      "minLength": 1,
      "title": "App",
      "type": "string"
    },
    "key_id": {
      "anyOf": [
        {
          "maxLength": 300,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Key Id"
    }
  },
  "required": [
    "app"
  ],
  "title": "RiverhogActor",
  "type": "object"
}
```
