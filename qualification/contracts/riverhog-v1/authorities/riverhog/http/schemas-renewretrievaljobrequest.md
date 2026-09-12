# schemas: RenewRetrievalJobRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-renewretrievaljobrequest:9184554874 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RenewRetrievalJobRequest`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- `title`: RenewRetrievalJobRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `lease_seconds` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c340f9b632598fe59aae3fc711a245ed0211b36c94b1b056c43b0a5bc95e419b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "lease_seconds": {
      "minimum": 1,
      "title": "Lease Seconds",
      "type": "integer"
    }
  },
  "required": [
    "lease_seconds"
  ],
  "title": "RenewRetrievalJobRequest",
  "type": "object"
}
```
