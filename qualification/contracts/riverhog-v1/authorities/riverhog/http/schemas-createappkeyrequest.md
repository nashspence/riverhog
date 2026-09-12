# schemas: CreateAppKeyRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-createappkeyrequest:8d7c1a2cf8 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CreateAppKeyRequest`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ApplicationAccessGrantSet](schemas-applicationaccessgrantset.md)

## Contract summary

- `title`: CreateAppKeyRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `access` | yes | #/components/schemas/ApplicationAccessGrantSet |  |
| `expires_in_seconds` | no | object (2 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2839386f89d3f9b0df156fcde75a23999719d77b217a25de451b7f40aa3c7125 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "access": {
      "$ref": "#/components/schemas/ApplicationAccessGrantSet"
    },
    "expires_in_seconds": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Expires In Seconds"
    }
  },
  "required": [
    "access"
  ],
  "title": "CreateAppKeyRequest",
  "type": "object"
}
```
