# schemas: ProcessingClaimFenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimfencedocument:46e4127246 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimFenceDocument`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- `title`: ProcessingClaimFenceDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `fence` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eabbed4a890891c7bfbd4c978da08de3bbd73ad40fb1a9fb9249a8a5ccf814d4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    }
  },
  "required": [
    "fence"
  ],
  "title": "ProcessingClaimFenceDocument",
  "type": "object"
}
```
