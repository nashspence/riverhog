# schemas: DiscardCollectionUploadRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-discardcollectionuploadrequest:6d32480092 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/DiscardCollectionUploadRequest`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- `title`: DiscardCollectionUploadRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `challenge` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d79783a6821604c4992d7a97f1007b46d65c25bc3f650494e5c4d29412ee3b5d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "challenge": {
      "minLength": 1,
      "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
      "title": "Challenge",
      "type": "string"
    }
  },
  "required": [
    "challenge"
  ],
  "title": "DiscardCollectionUploadRequest",
  "type": "object"
}
```
