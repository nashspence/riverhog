# schemas: ProcessingClaimAbandonDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimabandondocument:a22f22a1c6 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimAbandonDocument`

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
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: ProcessingClaimAbandonDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `fence` | yes | integer |  |
| `reason` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: db2136a2c3b36883b975822f17c927e5ac2386f19c8c8c8fa80ac47af99be5f0 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "reason": {
      "maxLength": 1000,
      "minLength": 1,
      "title": "Reason",
      "type": "string"
    }
  },
  "required": [
    "fence",
    "reason"
  ],
  "title": "ProcessingClaimAbandonDocument",
  "type": "object"
}
```
